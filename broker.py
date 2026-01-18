import os
import uuid
import math
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import List, Optional, Dict
from telegram_notifier import notifier_from_env

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

from state import BotState
from journal import TradeJournal


@dataclass
class InstrumentInfo:
    ticker: str
    figi: str
    lot: int
    min_price_increment: float


class Broker:
    """
    Broker wrapper for T-Invest Invest API.

    Features:
      - supports sandbox / real mode (cfg['use_sandbox'])
      - resolves tickers via share_by(TICKER, class_code='TQBR') -> avoids futures
      - instrument cache (figi -> InstrumentInfo)
      - price rounding to min_price_increment
      - idempotent orders (client order_id UUID)
      - safe state sync (positions + active orders + entry bookkeeping)
      - end-of-day flatten (cancel + close position)
      - CSV journal (signals/orders/fills/cancels)
      - order execution polling via get_order_state
    """

    def __init__(self, client: Client, cfg: dict):
        self.client = client
        self.cfg = cfg
        self.state = BotState()
        self.tg = notifier_from_env(enabled=bool(cfg.get("telegram_enabled", False)))

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
        self.class_code = cfg.get("class_code", "TQBR")

        # cache: figi -> InstrumentInfo
        self._figi_info: Dict[str, InstrumentInfo] = {}

        # backoff settings
        self._retry_sleep_min = float(cfg.get("retry_sleep_min", 1.0))
        self._retry_sleep_max = float(cfg.get("retry_sleep_max", 10.0))

        # CSV journal
        self.journal = TradeJournal(cfg.get("trades_csv", "logs/trades.csv"))

    # ---------- logging ----------
    def log(self, msg: str):
        self.logger.info(msg)
        print(msg)

    # ---------- journal helpers ----------
    def _ticker_for_figi(self, figi: str) -> str:
        info = self._figi_info.get(figi)
        return info.ticker if info else ""

    def journal_event(self, event: str, figi: str, **kwargs):
        self.journal.write(event=event, figi=figi, ticker=self._ticker_for_figi(figi), **kwargs)

    # ---------- time/day helpers ----------
    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo("UTC")).date().isoformat()

    def _ensure_day_rollover(self):
        today = self._today_key()
        if self.state.current_day != today:
            self.state.reset_day(today)

    # ---------- backoff wrapper ----------
    def _call(self, fn, *args, **kwargs):
        tries = int(self.cfg.get("retry_tries", 3))
        sleep = self._retry_sleep_min

        for attempt in range(1, tries + 1):
            try:
                return fn(*args, **kwargs)
            except RequestError as e:
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
        if self.use_sandbox:
            accs = self._call(self.client.sandbox.get_sandbox_accounts).accounts
            if not accs:
                self.log("[INFO] No sandbox accounts. Creating one...")
                created = self._call(self.client.sandbox.open_sandbox_account)
                account_id = created.account_id
                self.log(f"[INFO] Created sandbox account: {account_id}")

                init_rub = float(self.cfg.get("sandbox_pay_in_rub", 0.0))
                if init_rub > 0:
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

        resp = self._call(self.client.users.get_accounts)
        if not resp.accounts:
            raise RuntimeError("Нет доступных счетов")
        return resp.accounts[0].id

    @staticmethod
    def _money_value(amount: float, currency: str):
        q = decimal_to_quotation(Decimal(str(amount)))
        from tinkoff.invest import MoneyValue  # type: ignore
        return MoneyValue(units=q.units, nano=q.nano, currency=currency)

    # ---------- instruments ----------
    def resolve_instruments(self, tickers: List[str]) -> Dict[str, InstrumentInfo]:
        """
        Надёжное разрешение тикеров в акции MOEX (TQBR по умолчанию),
        чтобы не попадать на FUT/derivatives из find_instrument.
        """
        out: Dict[str, InstrumentInfo] = {}
        for t in tickers:
            try:
                r = self._call(
                    self.client.instruments.share_by,
                    id=t,
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                    class_code=self.class_code,
                )
                share = r.instrument
            except Exception as e:
                self.log(f"[WARN] share_by(TICKER) failed for {t} class={self.class_code}: {e}")
                continue

            figi = share.figi
            lot = int(share.lot)
            mpi = float(quotation_to_decimal(share.min_price_increment))

            info = InstrumentInfo(ticker=t, figi=figi, lot=lot, min_price_increment=mpi)
            out[t] = info
            self._figi_info[figi] = info

        return out

    def pick_tradeable_figis(self, universe_cfg: dict, max_lot_cost: float) -> List[str]:
        instruments = self.resolve_instruments(universe_cfg["tickers"])
        figis: List[str] = []

        for t, info in instruments.items():
            last_price = self.get_last_price(info.figi)
            if last_price is None:
                self.log(f"[SKIP] {t} no last price")
                continue

            lot_cost = last_price * info.lot
            if lot_cost <= max_lot_cost:
                figis.append(info.figi)
                self.log(f"[OK] {t} {info.figi} lot={info.lot} lot_cost≈{lot_cost:.2f}")
            else:
                self.log(f"[SKIP] {t} lot_cost≈{lot_cost:.2f} > {max_lot_cost:.2f}")

        return figis

    # ---------- price helpers ----------
    @staticmethod
    def _round_to_step_down(price: float, step: float) -> float:
        if step <= 0:
            return price
        return math.floor(price / step) * step

    def _normalize_price(self, figi: str, price: float) -> float:
        info = self._figi_info.get(figi)
        if not info:
            return price
        return self._round_to_step_down(price, info.min_price_increment)

    # ---------- market data ----------
    def get_last_price(self, figi: str) -> Optional[float]:
        try:
            r = self._call(self.client.market_data.get_last_prices, figi=[figi])
            if not r.last_prices:
                return None
            return float(quotation_to_decimal(r.last_prices[0].price))
        except Exception:
            return None

    def get_last_candles_1m(self, figi: str, lookback_minutes: int) -> Optional[pd.DataFrame]:
        to_ = now()
        from_ = to_ - timedelta(minutes=lookback_minutes + 5)

        try:
            candles = []
            for c in self.client.get_all_candles(
                figi=figi,
                from_=from_,
                to=to_,
                interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
            ):
                candles.append(c)

            if not candles:
                return None

            df = pd.DataFrame(
                {
                    "time": [x.time for x in candles],
                    "open": [float(quotation_to_decimal(x.open)) for x in candles],
                    "high": [float(quotation_to_decimal(x.high)) for x in candles],
                    "low": [float(quotation_to_decimal(x.low)) for x in candles],
                    "close": [float(quotation_to_decimal(x.close)) for x in candles],
                    "volume": [int(x.volume) for x in candles],
                }
            )
            return df
        except RequestError as e:
            self.log(f"[WARN] candles error {figi}: {e}")
            return None

    # ---------- orders / positions ----------
    def sync_state(self, account_id: str, figi: str):
        """
        Updates:
          - position lots for figi
          - active order id for figi
          - entry bookkeeping (MVP)
        """
        self._ensure_day_rollover()
        fs = self.state.get(figi)

        prev_lots = fs.position_lots
        new_lots = prev_lots

        # positions
        try:
            pos = self._call(self.client.operations.get_positions, account_id=account_id)
            lots = 0
            for sec in pos.securities:
                if sec.figi == figi:
                    lots = int(quotation_to_decimal(sec.balance))
                    break
            new_lots = lots
            fs.position_lots = new_lots
        except Exception as e:
            self.log(f"[WARN] get_positions failed: {e}")

        # active orders
        try:
            orders = self._call(self.client.orders.get_orders, account_id=account_id).orders
            active = [o for o in orders if o.figi == figi]
            fs.active_order_id = active[0].order_id if active else None
        except Exception as e:
            self.log(f"[WARN] get_orders failed: {e}")

        # entry bookkeeping
        if prev_lots > 0 and new_lots == 0:
            fs.entry_price = None
            fs.entry_time = None

        if prev_lots == 0 and new_lots > 0:
            if fs.entry_time is None:
                fs.entry_time = now()
            if fs.entry_price is None:
                last = self.get_last_price(figi)
                if last is not None:
                    fs.entry_price = float(last)

    def cancel_active_order(self, account_id: str, figi: str):
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return
        try:
            self._call(self.client.orders.cancel_order, account_id=account_id, order_id=fs.active_order_id)
            self.log(f"[CANCEL] {figi} order_id={fs.active_order_id}")

            self.journal_event(
                "CANCEL",
                figi,
                side="",
                lots=None,
                price=None,
                order_id=fs.active_order_id or "",
                client_uid=fs.client_order_uid or "",
                status="CANCELLED",
                reason="cancel_active_order",
            )

            fs.active_order_id = None
            fs.client_order_uid = None
        except Exception as e:
            self.log(f"[WARN] cancel_order failed: {e}")

    def place_limit_buy(self, account_id: str, figi: str, price: float, quantity_lots: int = 1) -> bool:
        if self.state.trades_today < 0:  # safeguard (never true)
            return False

        fs = self.state.get(figi)
        if fs.active_order_id:
            return False
        if fs.position_lots > 0:
            return False

        price = self._normalize_price(figi, price)
        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(price)

        try:
            r = self._call(
                self.client.orders.post_order,
                account_id=account_id,
                figi=figi,
                quantity=quantity_lots,
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_BUY,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            self.state.trades_today += 1

            self.log(f"[ORDER] BUY {figi} qty={quantity_lots} price={price} (client_uid={client_uid})")

            self.journal_event(
                "SUBMIT",
                figi,
                side="BUY",
                lots=quantity_lots,
                price=price,
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_buy",
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order BUY failed: {e}")
            return False

    def place_limit_sell_to_close(self, account_id: str, figi: str, price: float) -> bool:
        fs = self.state.get(figi)
        if fs.position_lots <= 0:
            return False

        if fs.active_order_id:
            self.cancel_active_order(account_id, figi)

        price = self._normalize_price(figi, price)
        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(price)

        try:
            r = self._call(
                self.client.orders.post_order,
                account_id=account_id,
                figi=figi,
                quantity=fs.position_lots,
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id

            self.log(f"[ORDER] SELL {figi} qty={fs.position_lots} price={price} (client_uid={client_uid})")

            self.journal_event(
                "SUBMIT",
                figi,
                side="SELL",
                lots=fs.position_lots,
                price=price,
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_sell_to_close",
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order SELL failed: {e}")
            return False

    # ---------- order execution polling ----------
    def poll_order_updates(self, account_id: str, figi: str):
        """
        Checks status of active order and writes fills/cancels/rejects into trades.csv.
        """
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return

        try:
            st = self._call(
                self.client.orders.get_order_state,
                account_id=account_id,
                order_id=fs.active_order_id,
            )
        except Exception as e:
            self.log(f"[WARN] get_order_state failed {figi}: {e}")
            return

        status = str(getattr(st, "execution_report_status", ""))
        lots_requested = int(getattr(st, "lots_requested", 0) or 0)
        lots_executed = int(getattr(st, "lots_executed", 0) or 0)
        direction = str(getattr(st, "direction", ""))

        avg_price = None
        ap = getattr(st, "average_position_price", None)
        if ap is not None:
            try:
                avg_price = float(quotation_to_decimal(ap))
            except Exception:
                avg_price = None

        side = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else "")

        # partial fill
        if lots_executed > 0 and lots_executed < lots_requested:
            self.journal_event(
                "PARTIAL_FILL",
                figi,
                side=side,
                lots=lots_executed,
                price=avg_price,
                order_id=fs.active_order_id,
                client_uid=fs.client_order_uid or "",
                status=status,
                reason="partial_fill",
                meta={"lots_requested": lots_requested},
            )
            self.tg.send(f"✅ FILL {self._ticker_for_figi(figi)} {side} lots={lots_executed} price={avg_price}", throttle_sec=0)
            self.tg.send(f"⛔ REJECT {self._ticker_for_figi(figi)} {side} status={status}", throttle_sec=1)

        final_statuses = {
            "EXECUTION_REPORT_STATUS_FILL",
            "EXECUTION_REPORT_STATUS_REJECTED",
            "EXECUTION_REPORT_STATUS_CANCELLED",
        }

        if status in final_statuses:
            if status == "EXECUTION_REPORT_STATUS_FILL":
                self.journal_event(
                    "FILL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=fs.active_order_id,
                    client_uid=fs.client_order_uid or "",
                    status=status,
                    reason="filled",
                )

                # entry bookkeeping
                if side == "BUY":
                    if fs.entry_time is None:
                        fs.entry_time = now()
                    if fs.entry_price is None and avg_price is not None:
                        fs.entry_price = avg_price
                elif side == "SELL":
                    fs.entry_price = None
                    fs.entry_time = None

            elif status == "EXECUTION_REPORT_STATUS_CANCELLED":
                self.journal_event(
                    "CANCEL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=fs.active_order_id,
                    client_uid=fs.client_order_uid or "",
                    status=status,
                    reason="cancelled_by_api",
                )

            elif status == "EXECUTION_REPORT_STATUS_REJECTED":
                self.journal_event(
                    "REJECT",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=fs.active_order_id,
                    client_uid=fs.client_order_uid or "",
                    status=status,
                    reason="rejected",
                )

            # clear active order
            fs.active_order_id = None
            fs.client_order_uid = None

    # ---------- flatten / pnl ----------
    def flatten_if_needed(self, account_id: str, schedule_cfg: dict):
        ts = now()
        if not self.flatten_due(ts, schedule_cfg):
            return

        for figi in list(self.state.figi.keys()):
            fs = self.state.get(figi)

            if fs.active_order_id:
                self.cancel_active_order(account_id, figi)

            if fs.position_lots > 0:
                last = self.get_last_price(figi)
                if last is None:
                    continue
                self.place_limit_sell_to_close(account_id, figi, price=last)

    def calc_day_cashflow(self, account_id: str) -> float:
        """
        MVP protective day metric:
        sum of operation payments for today in selected currency.
        """
        try:
            tz = ZoneInfo("Europe/Moscow")
            today_local = datetime.now(tz=tz).date()

            from_local = datetime.combine(today_local, datetime.min.time(), tzinfo=tz)
            to_local = datetime.combine(today_local, datetime.max.time(), tzinfo=tz)

            from_utc = from_local.astimezone(ZoneInfo("UTC"))
            to_utc = to_local.astimezone(ZoneInfo("UTC"))

            ops = self._call(
                self.client.operations.get_operations,
                account_id=account_id,
                from_=from_utc,
                to=to_utc,
            )

            total = 0.0
            for op in ops.operations:
                if op.payment.currency == self.currency:
                    total += float(quotation_to_decimal(op.payment))

            return total

        except Exception as e:
            self.log(f"[WARN] calc_day_cashflow failed: {e}")
            return 0.0
