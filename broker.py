import os
import uuid
import math
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Dict

import pandas as pd

from tinkoff.invest import (
    Client,
    CandleInterval,
    InstrumentIdType,
    OrderDirection,
    OrderType,
    Quotation,
    RequestError,
)
from tinkoff.invest.utils import now, quotation_to_decimal, decimal_to_quotation

from state import BotState, FigiState


@dataclass
class InstrumentInfo:
    ticker: str
    figi: str
    lot: int
    min_price_increment: float


class Broker:
    """
    Broker wrapper for T-Invest Invest API.

    Key features:
      - supports sandbox / real mode (cfg['use_sandbox'])
      - instrument cache (figi -> InstrumentInfo)
      - price rounding to min_price_increment
      - idempotent orders (client order_id UUID)
      - safe state sync (positions + active orders)
      - end-of-day flatten (cancel + close position)
      - simple API backoff
    """

    def __init__(self, client: Client, cfg: dict):
        self.client = client
        self.cfg = cfg
        self.state = BotState()

        # Logger (stable, no surprises with basicConfig/root propagation)
        os.makedirs("logs", exist_ok=True)
        self.logger = logging.getLogger("bot")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        log_path = cfg.get("log_file", "logs/bot.log")
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        self.logger.addHandler(fh)
        self.logger.propagate = False

        self.currency = cfg.get("currency", "rub")
        self.use_sandbox = bool(cfg.get("use_sandbox", True))

        # cache: figi -> InstrumentInfo
        self._figi_info: Dict[str, InstrumentInfo] = {}

        # backoff settings
        self._retry_sleep_min = float(cfg.get("retry_sleep_min", 1.0))
        self._retry_sleep_max = float(cfg.get("retry_sleep_max", 10.0))

    # ---------- logging ----------
    def log(self, msg: str):
        self.logger.info(msg)
        print(msg)

    # ---------- time/day helpers ----------
    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo("UTC")).date().isoformat()

    def _ensure_day_rollover(self):
        today = self._today_key()
        if self.state.current_day != today:
            self.state.current_day = today
            self.state.trades_today = 0
            self.state.day_locked = False

    # ---------- backoff wrapper ----------
    def _call(self, fn, *args, **kwargs):
        """
        Wrapper for API calls with simple retry/backoff on RequestError.
        """
        tries = int(self.cfg.get("retry_tries", 3))
        sleep = self._retry_sleep_min

        for attempt in range(1, tries + 1):
            try:
                return fn(*args, **kwargs)
            except RequestError as e:
                # Log and backoff
                self.log(f"[WARN] API error (attempt {attempt}/{tries}): {e}")
                if attempt == tries:
                    raise
                time.sleep(sleep)
                sleep = min(self._retry_sleep_max, sleep * 2)

    # ---------- schedule ----------
    def is_trading_time(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)

        start = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["start_trade"]), tzinfo=tz)
        flatten = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["flatten_time"]), tzinfo=tz)
        return start <= ts_local <= flatten

    def new_entries_allowed(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)
        stop_entries = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["stop_new_entries"]), tzinfo=tz)
        return ts_local <= stop_entries

    def flatten_due(self, ts_utc: datetime, schedule_cfg: dict) -> bool:
        tz = ZoneInfo(schedule_cfg["tz"])
        ts_local = ts_utc.astimezone(tz)
        flatten = datetime.combine(ts_local.date(), self._parse_hhmm(schedule_cfg["flatten_time"]), tzinfo=tz)
        return ts_local >= flatten

    @staticmethod
    def _parse_hhmm(s: str):
        hh, mm = s.split(":")
        return datetime.strptime(f"{hh}:{mm}", "%H:%M").time()

    # ---------- sandbox / accounts ----------
    def pick_account_id(self) -> str:
        """
        If sandbox mode: create sandbox account if none exists and return it.
        If real mode: return first real account.
        """
        if self.use_sandbox:
            # Sandbox accounts are managed by sandbox service
            try:
                accs = self._call(self.client.sandbox.get_sandbox_accounts).accounts
            except Exception as e:
                raise RuntimeError(f"Не удалось получить sandbox accounts: {e}")

            if not accs:
                self.log("[INFO] No sandbox accounts. Creating one...")
                created = self._call(self.client.sandbox.open_sandbox_account)
                account_id = created.account_id
                self.log(f"[INFO] Created sandbox account: {account_id}")

                # Optional: pay-in initial balance
                init_rub = float(self.cfg.get("sandbox_pay_in_rub", 100000.0))
                try:
                    self._call(
                        self.client.sandbox.sandbox_pay_in,
                        account_id=account_id,
                        amount=self._money_value(init_rub, self.currency),
                    )
                    self.log(f"[INFO] Sandbox pay-in: {init_rub:.2f} {self.currency}")
                except Exception as e:
                    self.log(f"[WARN] Sandbox pay-in failed: {e}")

                return account_id

            return accs[0].id

        # Real accounts
        resp = self._call(self.client.users.get_accounts)
        if not resp.accounts:
            raise RuntimeError("Нет доступных счетов")
        return resp.accounts[0].id

    @staticmethod
    def _money_value(amount: float, currency: str):
        """
        MoneyValue expects units + nano; easiest via decimal_to_quotation then map.
        """
        q = decimal_to_quotation(amount)
        # MoneyValue class is in tinkoff.invest, but constructing it directly
        # can be version-dependent. sandbox_pay_in accepts MoneyValue.
        # In recent SDK versions MoneyValue is available as tinkoff.invest.MoneyValue.
        # To stay robust, we
