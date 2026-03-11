import os
import csv
import json
import uuid
import math
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from decimal import Decimal
from typing import List, Optional, Dict, Any

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
    Broker wrapper for T-Invest with:
      - sandbox/real routing
      - ticker -> share resolution
      - candle polling (1m)
      - idempotent limit orders
      - journal
      - order polling (sandbox/real)
    """
    KEY_EVENT_MARKERS = (
        "[ERROR]",
        "[WARN]",
        "[ORDER]",
        "[FILL]",
        "[PNL]",
        "[CANCEL",
        "[REJECT",
        "[EXPIRE]",
        "[SIGNAL]",
        "Daily report",
        "Portfolio snapshot",
        "[INFO] Account:",
    )

    def __init__(
        self,
        client: Client,
        cfg: dict,
        notifier=None,
        notify_cfg: Optional[dict] = None,
        trading_tz: str = "Europe/Moscow",
    ):
        self.client = client
        self.cfg = cfg
        self.state = BotState()
        self.notifier = notifier
        self.notify_cfg = notify_cfg or {}
        self.trading_tz = trading_tz

        self._last_low_cash_warn: Dict[str, float] = {}
        self._last_notify_error_warn: float = 0.0
        self._reserved_rub_by_figi: Dict[str, float] = {}
        self._journaled_fill_order_ids: set[str] = set()

        os.makedirs("logs", exist_ok=True)

        self.logger = logging.getLogger("bot")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        log_path = cfg.get("log_file", "logs/bot.log")
        self.logger.addHandler(self._build_file_handler(log_path))
        self.logger.propagate = False

        self.key_logger = logging.getLogger("bot.key_events")
        self.key_logger.setLevel(logging.INFO)
        self.key_logger.handlers.clear()
        key_log_path = cfg.get("key_log_file", "logs/key_events.log")
        self.key_logger.addHandler(self._build_file_handler(key_log_path))
        self.key_logger.propagate = False

        self.currency = cfg.get("currency", "rub")
        self.use_sandbox = bool(cfg.get("use_sandbox", True))
        self.class_code = cfg.get("class_code", "TQBR")
        self.commission_pct = float(cfg.get("commission_pct", 0.04))
        self.commission_rate = float(self.commission_pct) / 100.0

        self._retry_tries = int(cfg.get("retry_tries", 3))
        self._retry_sleep_min = float(cfg.get("retry_sleep_min", 1.0))
        self._retry_sleep_max = float(cfg.get("retry_sleep_max", 10.0))

        # NEW: how close to last we want to place limit orders (ticks)
        self.buy_aggressive_ticks = int(cfg.get("buy_aggressive_ticks", 1))
        self.sell_aggressive_ticks = int(cfg.get("sell_aggressive_ticks", 1))

        self._figi_info: Dict[str, InstrumentInfo] = {}
        self.last_cash_rub: float = 0.0
        self.account_id: Optional[str] = None
        self.state_file = cfg.get("state_file", "logs/runtime_state.json")

        self.journal = TradeJournal(cfg.get("trades_csv", "logs/trades.csv"))
        self._bootstrap_journal_index()

    # ---------- logging ----------
    @staticmethod
    def _build_file_handler(path: str) -> logging.FileHandler:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        fh = logging.FileHandler(path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        return fh

    @classmethod
    def _is_key_event(cls, msg: str) -> bool:
        return any(marker in msg for marker in cls.KEY_EVENT_MARKERS)

    def log(self, msg: str):
        self.logger.info(msg)
        if self._is_key_event(msg):
            self.key_logger.info(msg)
        print(msg)

    def notify(self, text: str, throttle_sec: float = 0.0):
        if not self.notifier:
            return
        try:
            ok = bool(self.notifier.send(text, throttle_sec=throttle_sec))
            if not ok:
                now_ts = time.time()
                # Do not flood logs when Telegram is unstable.
                if now_ts - self._last_notify_error_warn >= 300:
                    self._last_notify_error_warn = now_ts
                    self.log("[WARN] Telegram send failed or skipped (check TG_BOT_TOKEN/TG_CHAT_ID/network)")
        except Exception as e:
            now_ts = time.time()
            if now_ts - self._last_notify_error_warn >= 300:
                self._last_notify_error_warn = now_ts
                self.log(f"[WARN] Telegram send error: {e}")

    def _notify_enabled(self, category: str) -> bool:
        compact_mode = bool(self.notify_cfg.get("compact_mode", True))
        defaults_compact = {
            "startup": True,
            "signal": False,
            "order": False,
            "fill": True,
            "pnl": True,
            "portfolio": False,
            "daily_report": True,
            "warning": True,
            "error": True,
            "service": True,
            "expire": False,
            "cancel": False,
            "reject": True,
        }
        defaults_verbose = {
            **defaults_compact,
            "signal": True,
            "portfolio": True,
            "expire": True,
            "cancel": True,
        }
        defaults = defaults_compact if compact_mode else defaults_verbose
        key_map = {
            "startup": "send_startup",
            "signal": "send_signals",
            "order": "send_orders",
            "fill": "send_fills",
            "pnl": "send_pnl",
            "portfolio": "send_portfolio_snapshots",
            "daily_report": "send_daily_report",
            "warning": "send_warnings",
            "error": "send_errors",
            "service": "send_service",
            "expire": "send_expires",
            "cancel": "send_cancels",
            "reject": "send_rejects",
        }
        cfg_key = key_map.get(category)
        if cfg_key and cfg_key in self.notify_cfg:
            return bool(self.notify_cfg.get(cfg_key))
        return bool(defaults.get(category, True))

    def notify_event(self, category: str, text: str, throttle_sec: float = 0.0):
        if not self._notify_enabled(category):
            return
        self.notify(text, throttle_sec=throttle_sec)

    @staticmethod
    def _fmt_rub(v: float) -> str:
        return f"{float(v):,.2f} RUB"

    def format_order_notification(self, side: str, figi: str, qty: int, price: float, cash: float, free: float) -> str:
        ticker = self._ticker_for_figi(figi) or figi
        return (
            f"Order {side}\n"
            f"Ticker: {ticker}\n"
            f"Lots: {int(qty)}\n"
            f"Price: {float(price):.4f}\n"
            f"Cash: {self._fmt_rub(cash)} | Free: {self._fmt_rub(free)}"
        )

    def format_signal_notification(
        self, action: str, figi: str, last: float, limit_price: float, cash: float, free: float, reason: str
    ) -> str:
        ticker = self._ticker_for_figi(figi) or figi
        return (
            f"Signal {action}\n"
            f"Ticker: {ticker}\n"
            f"Last: {float(last):.4f}\n"
            f"Limit: {float(limit_price):.4f}\n"
            f"Cash: {self._fmt_rub(cash)} | Free: {self._fmt_rub(free)}\n"
            f"Reason: {reason}"
        )

    def format_fill_notification(self, side: str, figi: str, lots_executed: int, avg_price: Optional[float]) -> str:
        ticker = self._ticker_for_figi(figi) or figi
        px = "N/A" if avg_price is None else f"{float(avg_price):.4f}"
        return f"Fill {side}\nTicker: {ticker}\nLots: {int(lots_executed)}\nPrice: {px}"

    @staticmethod
    def _emoji_for_pnl(value: float) -> str:
        if float(value) > 0:
            return "🟢"
        if float(value) < 0:
            return "🔴"
        return "⚪"

    def format_trade_fill_notification(
        self,
        side: str,
        figi: str,
        lots_executed: int,
        avg_price: Optional[float],
        reason: str = "",
        pnl_abs: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        commission_rub: Optional[float] = None,
    ) -> str:
        ticker = self._ticker_for_figi(figi) or figi
        px = "N/A" if avg_price is None else f"{float(avg_price):.4f}"
        reason_txt = reason or "n/a"
        if side == "BUY":
            return (
                "🟢 BUY исполнен\n"
                f"Тикер: {ticker}\n"
                f"Лоты: {int(lots_executed)}\n"
                f"Цена: {px}\n"
                f"Причина: {reason_txt}"
            )

        if side == "SELL":
            if pnl_abs is None or pnl_pct is None:
                return (
                    "⚪ SELL исполнен\n"
                    f"Тикер: {ticker}\n"
                    f"Лоты: {int(lots_executed)}\n"
                    f"Цена: {px}\n"
                    f"Причина: {reason_txt}"
                )
            emj = self._emoji_for_pnl(float(pnl_abs))
            comm_txt = ""
            if commission_rub is not None:
                comm_txt = f"\nКомиссия: {float(commission_rub):.2f} RUB"
            return (
                f"{emj} SELL исполнен\n"
                f"Тикер: {ticker}\n"
                f"Лоты: {int(lots_executed)}\n"
                f"Цена: {px}\n"
                f"Результат: {float(pnl_abs):+.2f} RUB ({float(pnl_pct):+.2f}%)\n"
                f"{comm_txt}\n"
                f"Причина: {reason_txt}"
            )

        return f"⚪ Сделка исполнена\nТикер: {ticker}\nЛоты: {int(lots_executed)}\nЦена: {px}\nПричина: {reason_txt}"

    def format_pnl_notification(self, figi: str, pnl_abs: float, pnl_pct: float, entry: float, exit_: float) -> str:
        ticker = self._ticker_for_figi(figi) or figi
        return (
            f"PnL {ticker}\n"
            f"Result: {float(pnl_abs):+.2f} RUB ({float(pnl_pct):+.2f}%)\n"
            f"Entry: {float(entry):.4f}\n"
            f"Exit: {float(exit_):.4f}"
        )

    def build_portfolio_status_telegram(self, account_id: str, figis: List[str], title: str = "Portfolio") -> str:
        self.refresh_account_snapshot(account_id, figis)

        cash = float(self.get_cached_cash_rub(account_id))
        reserved = float(self._reserved_rub_total())
        free = float(max(0.0, cash - reserved))

        lines: List[str] = [
            title,
            f"Cash: {self._fmt_rub(cash)}",
            f"Free: {self._fmt_rub(free)}",
            f"Reserved: {self._fmt_rub(reserved)}",
            "Positions:",
        ]

        any_pos = False
        for figi in figis:
            fs = self.state.get(figi)
            lots = int(fs.position_lots)
            if lots <= 0:
                continue
            any_pos = True

            ticker = self._ticker_for_figi(figi) or figi
            last = self.get_last_price(figi)
            entry = fs.entry_price

            if entry is None or last is None:
                lines.append(f"- {ticker}: lots={lots}, entry=N/A, last={last if last is not None else 'N/A'}")
                continue

            lot_size = self._lot_size(figi)
            pnl_abs = (float(last) - float(entry)) * float(lot_size) * float(lots)
            pnl_pct = (float(last) / float(entry) - 1.0) * 100.0
            lines.append(
                f"- {ticker}: lots={lots}, entry={float(entry):.4f}, last={float(last):.4f}, "
                f"PnL={pnl_abs:+.2f} RUB ({pnl_pct:+.2f}%)"
            )

        if not any_pos:
            lines.append("- no positions")

        return "\n".join(lines)

    def build_intraday_report_telegram(self, account_id: str, figis: List[str], title: str = "Промежуточный отчет") -> str:
        self.refresh_account_snapshot(account_id, figis)

        cash = float(self.get_cached_cash_rub(account_id))
        reserved = float(self._reserved_rub_total())
        free = float(max(0.0, cash - reserved))

        total_market_value = 0.0
        total_unrealized = 0.0
        pos_lines: List[str] = []

        for figi in figis:
            fs = self.state.get(figi)
            lots = int(fs.position_lots)
            if lots <= 0:
                continue

            ticker = self._ticker_for_figi(figi) or figi
            last = self.get_last_price(figi)
            lot_size = self._lot_size(figi)

            if last is None:
                pos_lines.append(f"- {ticker}: lots={lots}, last=N/A, PnL=N/A")
                continue

            market_value = float(last) * float(lot_size) * float(lots)
            total_market_value += market_value

            entry = fs.entry_price
            if entry is None:
                pos_lines.append(f"- {ticker}: lots={lots}, last={float(last):.4f}, PnL=N/A (entry unknown)")
                continue

            pnl_abs = (float(last) - float(entry)) * float(lot_size) * float(lots)
            pnl_pct = (float(last) / float(entry) - 1.0) * 100.0
            total_unrealized += float(pnl_abs)
            emj = self._emoji_for_pnl(float(pnl_abs))
            pos_lines.append(
                f"- {ticker}: lots={lots}, entry={float(entry):.4f}, last={float(last):.4f}, "
                f"{emj} {pnl_abs:+.2f} RUB ({pnl_pct:+.2f}%)"
            )

        equity_estimate = float(cash + total_market_value)
        realized = float(getattr(self.state, "day_realized_pnl_rub", 0.0) or 0.0)
        day_total = float(realized + total_unrealized)
        day_emoji = self._emoji_for_pnl(day_total)

        lines: List[str] = [
            title,
            f"Баланс (оценка): {self._fmt_rub(equity_estimate)}",
            f"Cash: {self._fmt_rub(cash)} | Free: {self._fmt_rub(free)} | Reserved: {self._fmt_rub(reserved)}",
            f"Результат дня (realized+unrealized): {day_emoji} {day_total:+.2f} RUB",
            f"Realized: {realized:+.2f} RUB | Unrealized: {total_unrealized:+.2f} RUB",
            "Позиции:",
        ]
        if pos_lines:
            lines.extend(pos_lines)
        else:
            lines.append("- нет открытых позиций")
        return "\n".join(lines)

    # ---------- converters ----------
    @staticmethod
    def _to_float(x: Any) -> float:
        if x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, Decimal):
            return float(x)
        units = getattr(x, "units", None)
        nano = getattr(x, "nano", None)
        if isinstance(units, int) and isinstance(nano, int):
            return float(units) + float(nano) / 1e9
        try:
            return float(quotation_to_decimal(x))
        except Exception:
            return 0.0

    def _extract_commission_from_order_state(self, st: Any) -> float:
        # API versions may expose different fields; prefer one total commission field.
        for name in ("executed_commission", "service_commission", "initial_commission", "commission"):
            v = getattr(st, name, None)
            if v is None:
                continue
            c = float(self._to_float(v))
            if abs(c) > 0:
                return float(abs(c))
        return 0.0

    def _calc_fixed_commission_rub(self, figi: str, lots_executed: int, avg_price: Optional[float]) -> float:
        if avg_price is None or int(lots_executed) <= 0:
            return 0.0
        lot_size = self._lot_size(figi)
        turnover = float(avg_price) * float(lot_size) * float(int(lots_executed))
        return float(abs(turnover) * float(self.commission_rate))

    # ---------- lot helpers ----------
    def _lot_size(self, figi: str) -> int:
        info = self._figi_info.get(figi)
        return int(info.lot) if info and int(info.lot) > 0 else 1

    def _balance_to_lots(self, figi: str, balance_value: Any) -> int:
        bal = float(self._to_float(balance_value))
        lot = self._lot_size(figi)
        if lot <= 1:
            return int(bal)
        lots = int(math.floor(bal / float(lot) + 1e-12))
        return max(0, lots)

    # ---------- journal helpers ----------
    def _ticker_for_figi(self, figi: str) -> str:
        info = self._figi_info.get(figi)
        return info.ticker if info else ""

    def format_instrument(self, figi: str) -> str:
        t = self._ticker_for_figi(figi)
        return f"{t} ({figi})" if t else figi

    def journal_event(self, event: str, figi: str, **kwargs):
        self.journal.write(event=event, figi=figi, ticker=self._ticker_for_figi(figi), **kwargs)
        if event == "FILL":
            oid = str(kwargs.get("order_id", "") or "").strip()
            if oid:
                self._journaled_fill_order_ids.add(oid)

    def _bootstrap_journal_index(self):
        path = Path(self.journal.path)
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    if str(row.get("event", "")).upper() != "FILL":
                        continue
                    oid = str(row.get("order_id", "") or "").strip()
                    if oid:
                        self._journaled_fill_order_ids.add(oid)
        except Exception:
            pass

    def save_runtime_state(self):
        payload = {
            "saved_at_utc": datetime.now(timezone.utc).isoformat(),
            "account_id": self.account_id,
            "state": self.state.to_dict(),
        }
        p = Path(self.state_file)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(p)

    def load_runtime_state(self, account_id: str):
        p = Path(self.state_file)
        if not p.exists():
            return
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            saved_account = str(payload.get("account_id", "") or "")
            if saved_account and saved_account != str(account_id):
                self.log(f"[INFO] Skip state restore: account mismatch ({saved_account} != {account_id})")
                return
            state_payload = payload.get("state", {}) or {}
            self.state.load_dict(state_payload)
            self.log(f"[INFO] Runtime state restored: {self.state_file}")
        except Exception as e:
            self.log(f"[WARN] Runtime state restore failed: {e}")

    # ---------- day helpers ----------
    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo(self.trading_tz)).date().isoformat()

    def _ensure_day_rollover(self):
        today = self._today_key()
        if self.state.current_day != today:
            self.state.reset_day(today)

    # ---------- retry wrapper ----------
    def _call(self, fn, *args, **kwargs):
        sleep = self._retry_sleep_min
        for attempt in range(1, self._retry_tries + 1):
            try:
                return fn(*args, **kwargs)
            except RequestError as e:
                self.log(f"[WARN] API error (attempt {attempt}/{self._retry_tries}): {e}")
                if attempt == self._retry_tries:
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

    # ---------- routing helpers ----------
    def _positions_call(self):
        return self.client.sandbox.get_sandbox_positions if self.use_sandbox else self.client.operations.get_positions

    def _orders_list_call(self):
        return self.client.sandbox.get_sandbox_orders if self.use_sandbox else self.client.orders.get_orders

    def _order_post_call(self):
        return self.client.sandbox.post_sandbox_order if self.use_sandbox else self.client.orders.post_order

    def _order_cancel_call(self):
        return self.client.sandbox.cancel_sandbox_order if self.use_sandbox else self.client.orders.cancel_order

    def _order_state_call(self):
        return self.client.sandbox.get_sandbox_order_state if self.use_sandbox else self.client.orders.get_order_state

    def _operations_call(self):
        return self.client.sandbox.get_sandbox_operations if self.use_sandbox else self.client.operations.get_operations

    # ---------- accounts / sandbox ----------
    def pick_account_id(self) -> str:
        configured_account_id = str(self.cfg.get("account_id", "") or "").strip()

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

                self.account_id = account_id
                return account_id

            acc_ids = [str(a.id) for a in accs]
            if configured_account_id:
                if configured_account_id not in acc_ids:
                    raise RuntimeError(
                        f"Configured sandbox account_id={configured_account_id} not found. Available: {acc_ids}"
                    )
                self.account_id = configured_account_id
                return configured_account_id

            self.account_id = accs[0].id
            return accs[0].id

        resp = self._call(self.client.users.get_accounts)
        if not resp.accounts:
            raise RuntimeError("Нет доступных счетов")
        acc_ids = [str(a.id) for a in resp.accounts]
        if configured_account_id:
            if configured_account_id not in acc_ids:
                raise RuntimeError(
                    f"Configured real account_id={configured_account_id} not found. Available: {acc_ids}"
                )
            self.account_id = configured_account_id
            return configured_account_id

        raise RuntimeError(
            "Для реального режима нужно явно задать broker.account_id в config.yaml. "
            f"Available accounts: {acc_ids}"
        )

    @staticmethod
    def _money_value(amount: float, currency: str):
        q = decimal_to_quotation(Decimal(str(float(amount))))
        from tinkoff.invest import MoneyValue  # type: ignore
        return MoneyValue(units=q.units, nano=q.nano, currency=currency)

    # ---------- cash helpers ----------
    def get_cash_rub(self, account_id: str) -> float:
        try:
            pos = self._call(self._positions_call(), account_id=account_id)
            cash = 0.0
            for m in pos.money:
                if m.currency == self.currency:
                    cash += float(self._to_float(m))
            return float(cash)
        except Exception as e:
            self.log(f"[WARN] get_cash_rub failed: {e}")
            return 0.0

    def get_cached_cash_rub(self, account_id: str | None = None) -> float:
        if self.last_cash_rub > 0:
            return float(self.last_cash_rub)
        if account_id:
            return float(self.get_cash_rub(account_id))
        return 0.0

    def _reserved_rub_total(self) -> float:
        return float(sum(float(v) for v in self._reserved_rub_by_figi.values()))

    def get_free_cash_rub_estimate(self, account_id: str | None = None) -> float:
        cash = self.get_cached_cash_rub(account_id)
        reserved = self._reserved_rub_total()
        return float(max(0.0, float(cash) - float(reserved)))

    # ---------- sandbox cash helpers ----------
    def ensure_sandbox_cash(self, account_id: str, min_cash_rub: float):
        if not self.use_sandbox:
            return

        min_cash_rub = float(min_cash_rub)
        cash = self.get_cash_rub(account_id)
        if cash >= min_cash_rub:
            self.log(f"[INFO] Sandbox cash OK: {cash:.2f} {self.currency}")
            return

        topup = max(0.0, min_cash_rub - cash)
        try:
            self._call(
                self.client.sandbox.sandbox_pay_in,
                account_id=account_id,
                amount=self._money_value(topup, self.currency),
            )
            self.log(f"[INFO] Sandbox pay-in: +{topup:.2f} {self.currency} (cash was {cash:.2f})")
        except Exception as e:
            self.log(f"[WARN] Sandbox pay-in failed: {e}")

    # ---------- account snapshot ----------
    def refresh_account_snapshot(self, account_id: str, figis: List[str]):
        self._ensure_day_rollover()
        figi_set = set(figis)

        # Positions
        try:
            pos = self._call(self._positions_call(), account_id=account_id)

            cash = 0.0
            for m in getattr(pos, "money", []) or []:
                if getattr(m, "currency", None) == self.currency:
                    cash += float(self._to_float(m))
            self.last_cash_rub = float(cash)

            by_figi_lots: Dict[str, int] = {f: 0 for f in figi_set}
            for sec in getattr(pos, "securities", []) or []:
                f = getattr(sec, "figi", "")
                if f in figi_set:
                    bal = getattr(sec, "balance", 0)
                    by_figi_lots[f] = int(self._balance_to_lots(f, bal))

            for f in figi_set:
                fs = self.state.get(f)
                prev_lots = int(fs.position_lots)
                fs.position_lots = int(by_figi_lots.get(f, 0))
                if prev_lots > 0 and int(fs.position_lots) == 0:
                    fs.entry_price = None
                    fs.entry_time = None
        except Exception as e:
            self.log(f"[WARN] get_positions failed: {e}")

        # Orders
        try:
            orders = self._call(self._orders_list_call(), account_id=account_id).orders
            active_by_figi: Dict[str, Dict[str, Any]] = {}
            for o in orders:
                f = getattr(o, "figi", "")
                if f in figi_set and f not in active_by_figi:
                    direction = str(getattr(o, "direction", ""))
                    side = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else "")
                    order_date = getattr(o, "order_date", None)
                    active_by_figi[f] = {
                        "order_id": getattr(o, "order_id", ""),
                        "side": side,
                        "order_date": order_date,
                    }

            for f in figi_set:
                fs = self.state.get(f)
                api_order = active_by_figi.get(f)
                if api_order and api_order.get("order_id"):
                    fs.active_order_id = api_order.get("order_id") or fs.active_order_id
                    if not fs.order_side and api_order.get("side"):
                        fs.order_side = api_order.get("side")
                    if fs.order_placed_ts is None:
                        od = api_order.get("order_date")
                        if isinstance(od, datetime):
                            fs.order_placed_ts = od
                            if not fs.active_order_reason:
                                fs.active_order_reason = "restored_from_api"
                elif fs.active_order_id is None:
                    self._reserved_rub_by_figi.pop(f, None)
        except Exception as e:
            self.log(f"[WARN] get_orders failed: {e}")

    # ---------- instruments ----------
    def resolve_instruments(self, tickers: List[str]) -> Dict[str, InstrumentInfo]:
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
                self.log(f"[WARN] share_by failed for {t}: {e}")
                continue

            figi = share.figi
            lot = int(share.lot)
            mpi = float(self._to_float(share.min_price_increment))

            info = InstrumentInfo(ticker=t, figi=figi, lot=lot, min_price_increment=float(mpi))
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

            lot_cost = float(last_price) * int(info.lot)
            if lot_cost <= float(max_lot_cost):
                figis.append(info.figi)
                self.log(f"[OK] {t} {info.figi} lot={info.lot} lot_cost≈{lot_cost:.2f}")
            else:
                self.log(f"[SKIP] {t} lot_cost≈{lot_cost:.2f} > {float(max_lot_cost):.2f}")

        return figis

    # ---------- rounding ----------
    @staticmethod
    def _round_to_step_down(price: float, step: float) -> float:
        if step <= 0:
            return float(price)
        return float(math.floor(float(price) / float(step)) * float(step))

    @staticmethod
    def _round_to_step_up(price: float, step: float) -> float:
        if step <= 0:
            return float(price)
        return float(math.ceil(float(price) / float(step)) * float(step))

    def _normalize_price(self, figi: str, price: float, side: str) -> float:
        info = self._figi_info.get(figi)
        if not info:
            return float(price)
        step = float(info.min_price_increment) if info.min_price_increment else 0.0
        p = float(price)
        if side.upper() == "BUY":
            return float(self._round_to_step_up(p, step))
        return float(self._round_to_step_down(p, step))

    # NEW: "closest to current" limit price in ticks
    def _aggressive_near_last(self, figi: str, side: str, suggested_price: float) -> float:
        """
        Make price максимально близко к last:
          BUY -> around last + buy_aggressive_ticks * step (rounded up)
          SELL -> around last - sell_aggressive_ticks * step (rounded down)
        If last is unavailable, fall back to suggested_price.
        """
        info = self._figi_info.get(figi)
        step = float(info.min_price_increment) if info and info.min_price_increment else 0.0
        last = self.get_last_price(figi)

        if last is None or step <= 0:
            return float(self._normalize_price(figi, float(suggested_price), side=side))

        if side.upper() == "BUY":
            target = float(last) + float(self.buy_aggressive_ticks) * step
            # keep not worse than suggested (so if strategy wants higher, allow it)
            p = max(float(suggested_price), target)
            return float(self._normalize_price(figi, p, side="BUY"))

        # SELL
        target = float(last) - float(self.sell_aggressive_ticks) * step
        p = min(float(suggested_price), target)
        return float(self._normalize_price(figi, p, side="SELL"))

    # ---------- market data ----------
    def get_last_price(self, figi: str) -> Optional[float]:
        try:
            r = self._call(self.client.market_data.get_last_prices, figi=[figi])
            if not r.last_prices:
                return None
            return float(self._to_float(r.last_prices[0].price))
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
                    "open": [float(self._to_float(x.open)) for x in candles],
                    "high": [float(self._to_float(x.high)) for x in candles],
                    "low": [float(self._to_float(x.low)) for x in candles],
                    "close": [float(self._to_float(x.close)) for x in candles],
                    "volume": [int(x.volume) for x in candles],
                }
            )
            return df
        except RequestError as e:
            self.log(f"[WARN] candles error {figi}: {e}")
            return None

    # ---------- portfolio status ----------
    def build_portfolio_status(self, account_id: str, figis: List[str], title: str = "") -> str:
        self.refresh_account_snapshot(account_id, figis)

        cash = float(self.get_cached_cash_rub(account_id))
        reserved = float(self._reserved_rub_total())
        free = float(max(0.0, cash - reserved))

        lines: List[str] = []
        if title:
            lines.append(title)
        lines.append("------------------------------------------------------------")
        lines.append(f"Cash: {cash:,.2f} RUB | Free≈{free:,.2f} | Reserved≈{reserved:,.2f}")
        lines.append("Positions:")

        any_pos = False
        for figi in figis:
            fs = self.state.get(figi)
            lots = int(fs.position_lots)
            if lots <= 0:
                continue
            any_pos = True

            lot_size = self._lot_size(figi)
            last = self.get_last_price(figi)
            ticker = self._ticker_for_figi(figi) or figi

            entry = fs.entry_price
            if entry is None or last is None:
                lines.append(f"  {ticker:<5} lots={lots:<3} entry=N/A last={last if last is not None else 'N/A'}")
                continue

            pnl_abs = (float(last) - float(entry)) * float(lot_size) * float(lots)
            pnl_pct = (float(last) / float(entry) - 1.0) * 100.0

            lines.append(
                f"  {ticker:<5} lots={lots:<3} entry={float(entry):.4f} last={float(last):.4f} "
                f"PnL={pnl_abs:+.2f} RUB ({pnl_pct:+.2f}%)"
            )

        if not any_pos:
            lines.append("  (no positions)")

        return "\n".join(lines)

    # ---------- orders ----------
    def _is_not_found_error(self, e: Exception) -> bool:
        s = str(e).upper()
        return ("NOT_FOUND" in s) or ("ORDER NOT FOUND" in s)

    def _recover_missing_fill_from_snapshot(
        self,
        figi: str,
        order_side: str,
        order_id: str,
        client_uid: str,
        order_reason: str,
    ) -> bool:
        fs = self.state.get(figi)
        side = str(order_side or "").upper()
        lots = int(getattr(fs, "position_lots", 0) or 0)
        if side == "BUY" and lots > 0:
            self.log(
                f"[RECOVER] BUY fill inferred from position snapshot {self.format_instrument(figi)} "
                f"lots={lots} order_id={order_id}"
            )
            self.journal_event(
                "FILL",
                figi,
                side="BUY",
                lots=lots,
                price=fs.entry_price,
                order_id=order_id,
                client_uid=client_uid,
                status="RECOVERED_FROM_SNAPSHOT",
                reason=order_reason or "filled_after_not_found",
                meta={"recovered": True},
            )
            if fs.entry_time is None:
                fs.entry_time = now()
            self.notify_event(
                "fill",
                self.format_trade_fill_notification(
                    side="BUY",
                    figi=figi,
                    lots_executed=lots,
                    avg_price=fs.entry_price,
                    reason=order_reason or "filled_after_not_found",
                    commission_rub=float(getattr(fs, "entry_commission_rub", 0.0) or 0.0),
                ),
                throttle_sec=0,
            )
            return True

        if side == "SELL" and lots == 0:
            self.log(
                f"[RECOVER] SELL fill inferred from position snapshot {self.format_instrument(figi)} "
                f"order_id={order_id}"
            )
            self.journal_event(
                "FILL",
                figi,
                side="SELL",
                lots=1,
                price=None,
                order_id=order_id,
                client_uid=client_uid,
                status="RECOVERED_FROM_SNAPSHOT",
                reason=order_reason or "filled_after_not_found",
                meta={"recovered": True},
            )
            self.notify_event(
                "fill",
                self.format_trade_fill_notification(
                    side="SELL",
                    figi=figi,
                    lots_executed=1,
                    avg_price=None,
                    reason=order_reason or "filled_after_not_found",
                    pnl_abs=None,
                    pnl_pct=None,
                    commission_rub=0.0,
                ),
                throttle_sec=0,
            )
            fs.entry_price = None
            fs.entry_time = None
            fs.entry_commission_rub = 0.0
            return True

        return False

    def cancel_active_order(self, account_id: str, figi: str, reason: str = "cancel_active_order") -> bool:
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return True

        oid = fs.active_order_id
        cuid = fs.client_order_uid or ""
        side = str(getattr(fs, "order_side", "") or "")
        order_reason = str(getattr(fs, "active_order_reason", "") or "")

        try:
            self._call(self._order_cancel_call(), account_id=account_id, order_id=oid)
            self.log(f"[CANCEL] {self.format_instrument(figi)} order_id={oid}")
            self.notify_event("cancel", f"[CANCEL] {self._ticker_for_figi(figi) or figi} order_id={oid}", throttle_sec=0)

            self.journal_event(
                "CANCEL",
                figi,
                side=str(getattr(fs, "order_side", "") or ""),
                lots=None,
                price=None,
                order_id=oid,
                client_uid=cuid,
                status="CANCELLED",
                reason=reason,
            )
            self.state.clear_order(figi)
            self._reserved_rub_by_figi.pop(figi, None)
            return True
        except Exception as e:
            if self._is_not_found_error(e):
                self.log(f"[CANCEL] {self.format_instrument(figi)} order_id={oid} already gone (NOT_FOUND)")
                self.refresh_account_snapshot(account_id, [figi])
                self._recover_missing_fill_from_snapshot(figi, side, oid, cuid, order_reason)
                self.state.clear_order(figi)
                self._reserved_rub_by_figi.pop(figi, None)
                return True
            else:
                self.log(f"[WARN] cancel_order failed: {e}")
                self.notify_event(
                    "warning",
                    f"[WARN] cancel failed: {self._ticker_for_figi(figi) or figi} | {e}",
                    throttle_sec=120,
                )
                return False

    def place_limit_buy(
        self,
        account_id: str,
        figi: str,
        price: float,
        quantity_lots: int = 1,
        reason: str = "",
    ) -> bool:
        fs = self.state.get(figi)
        if fs.active_order_id:
            return False
        if int(fs.position_lots) > 0:
            return False

        # NEW: price near last (ticks)
        price_f = self._aggressive_near_last(figi, "BUY", float(price))

        lot_size = self._lot_size(figi)
        est_cost = float(price_f) * float(lot_size) * float(quantity_lots)

        cash = self.get_cached_cash_rub(account_id) or self.get_cash_rub(account_id)
        free_cash = float(max(0.0, float(cash) - float(self._reserved_rub_total())))

        if cash > 0 and free_cash < est_cost * 1.01:
            now_ts = time.time()
            last_warn = self._last_low_cash_warn.get(figi, 0.0)
            if now_ts - last_warn >= 300:
                self._last_low_cash_warn[figi] = now_ts
                msg = (
                    f"[SKIP] BUY {self.format_instrument(figi)}: not enough FREE cash "
                    f"(cash={cash:.2f} reserved≈{self._reserved_rub_total():.2f} free≈{free_cash:.2f} need≈{est_cost:.2f})"
                )
                self.log(msg)
                self.notify_event("warning", msg, throttle_sec=0)

            self.journal_event(
                "SKIP",
                figi,
                side="BUY",
                lots=int(quantity_lots),
                price=float(price_f),
                order_id=None,
                client_uid=None,
                status="NO_CASH",
                reason="insufficient_free_cash_precheck",
                meta={"cash": cash, "reserved": self._reserved_rub_total(), "free": free_cash, "need": est_cost},
            )
            return False

        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(Decimal(str(price_f)))

        try:
            r = self._call(
                self._order_post_call(),
                account_id=account_id,
                figi=figi,
                quantity=int(quantity_lots),
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_BUY,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            fs.order_side = "BUY"
            fs.order_placed_ts = now()
            fs.active_order_reason = str(reason or "")

            self._reserved_rub_by_figi[figi] = float(est_cost)

            inst = self.format_instrument(figi)
            cash2 = self.get_cached_cash_rub(account_id)
            free2 = self.get_free_cash_rub_estimate(account_id)
            self.log(
                f"[ORDER] BUY {inst} qty={int(quantity_lots)} price={price_f} "
                f"| cash≈{cash2:.2f} free≈{free2:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.journal_event(
                "SUBMIT",
                figi,
                side="BUY",
                lots=int(quantity_lots),
                price=float(price_f),
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_buy",
                meta={"est_cost": est_cost, "signal_reason": str(reason or "")},
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order BUY failed: {e}")
            self.notify_event(
                "warning",
                f"[WARN] BUY submit failed: {self._ticker_for_figi(figi) or figi} | {e}",
                throttle_sec=120,
            )
            self.state.clear_order(figi)
            self._reserved_rub_by_figi.pop(figi, None)
            return False

    def place_limit_sell_to_close(self, account_id: str, figi: str, price: float, reason: str = "") -> bool:
        # Refresh this figi snapshot right before SELL to reduce stale-position rejects.
        self.refresh_account_snapshot(account_id, [figi])
        fs = self.state.get(figi)
        if int(fs.position_lots) <= 0:
            msg = f"[SKIP] SELL {self.format_instrument(figi)}: no position to close"
            self.log(msg)
            self.journal_event(
                "SKIP",
                figi,
                side="SELL",
                lots=0,
                price=float(price),
                order_id=None,
                client_uid=None,
                status="NO_POSITION",
                reason="sell_without_position_precheck",
                meta={"signal_reason": str(reason or "")},
            )
            return False

        if fs.active_order_id:
            if not self.cancel_active_order(account_id, figi, reason="replace_before_sell"):
                return False

        # NEW: price near last (ticks)
        price_f = self._aggressive_near_last(figi, "SELL", float(price))

        client_uid = str(uuid.uuid4())
        q = decimal_to_quotation(Decimal(str(price_f)))

        try:
            r = self._call(
                self._order_post_call(),
                account_id=account_id,
                figi=figi,
                quantity=int(fs.position_lots),
                price=Quotation(units=q.units, nano=q.nano),
                direction=OrderDirection.ORDER_DIRECTION_SELL,
                order_type=OrderType.ORDER_TYPE_LIMIT,
                order_id=client_uid,
            )

            fs.client_order_uid = client_uid
            fs.active_order_id = r.order_id
            fs.order_side = "SELL"
            fs.order_placed_ts = now()
            fs.active_order_reason = str(reason or "")

            self._reserved_rub_by_figi.pop(figi, None)

            inst = self.format_instrument(figi)
            cash = self.get_cached_cash_rub(account_id)
            free = self.get_free_cash_rub_estimate(account_id)
            self.log(
                f"[ORDER] SELL {inst} qty={int(fs.position_lots)} price={price_f} "
                f"| cash≈{cash:.2f} free≈{free:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.journal_event(
                "SUBMIT",
                figi,
                side="SELL",
                lots=int(fs.position_lots),
                price=float(price_f),
                order_id=r.order_id,
                client_uid=client_uid,
                status="NEW",
                reason="limit_sell_to_close",
                meta={"signal_reason": str(reason or "")},
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order SELL failed: {e}")
            self.notify_event(
                "warning",
                f"[WARN] SELL submit failed: {self._ticker_for_figi(figi) or figi} | {e}",
                throttle_sec=120,
            )
            self.state.clear_order(figi)
            return False

    # ---------- TTL expire ----------
    def expire_stale_orders(self, account_id: str, figi: str, ttl_sec: int) -> bool:
        fs = self.state.get(figi)
        if not fs.active_order_id or not fs.order_placed_ts:
            return False

        age = (now() - fs.order_placed_ts).total_seconds()
        if age < float(ttl_sec):
            return False

        inst = self.format_instrument(figi)
        self.log(f"[EXPIRE] {inst} order_id={fs.active_order_id} age={age:.0f}s ttl={ttl_sec}s -> cancelling")
        self.notify_event("expire", f"[EXPIRE] {inst} age={age:.0f}s -> cancel", throttle_sec=0)

        self.journal_event(
            "EXPIRE",
            figi,
            side=str(getattr(fs, "order_side", "") or ""),
            lots=None,
            price=None,
            order_id=fs.active_order_id,
            client_uid=fs.client_order_uid,
            status="EXPIRED",
            reason="ttl_expired",
            meta={"age_sec": float(age), "ttl_sec": int(ttl_sec)},
        )

        self.cancel_active_order(account_id, figi, reason="ttl_expired")
        return True

    def reprice_stale_order(self, account_id: str, figi: str, reprice_sec: int) -> bool:
        fs = self.state.get(figi)
        if reprice_sec <= 0 or not fs.active_order_id or not fs.order_placed_ts:
            return False

        age = (now() - fs.order_placed_ts).total_seconds()
        if age < float(reprice_sec):
            return False

        side = str(getattr(fs, "order_side", "") or "").upper()
        reason = str(getattr(fs, "active_order_reason", "") or "")
        last_price = self.get_last_price(figi)
        if last_price is None:
            return False

        inst = self.format_instrument(figi)
        self.log(
            f"[REPRICE] {inst} side={side} order_id={fs.active_order_id} age={age:.0f}s "
            f"-> replace near market"
        )
        self.journal_event(
            "REPRICE",
            figi,
            side=side,
            lots=int(getattr(fs, "position_lots", 0) or 0) if side == "SELL" else 1,
            price=float(last_price),
            order_id=fs.active_order_id,
            client_uid=fs.client_order_uid,
            status="REPRICE",
            reason=reason or "reprice_stale_order",
            meta={"age_sec": float(age), "reprice_sec": int(reprice_sec)},
        )

        if side == "BUY":
            if not self.cancel_active_order(account_id, figi, reason="reprice_replace_buy"):
                return False
            return self.place_limit_buy(account_id, figi, float(last_price), reason=reason)

        if side == "SELL":
            if not self.cancel_active_order(account_id, figi, reason="reprice_replace_sell"):
                return False
            return self.place_limit_sell_to_close(account_id, figi, float(last_price), reason=reason)

        return False

    # ---------- order state polling ----------
    def poll_order_updates(self, account_id: str, figi: str):
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return

        oid = fs.active_order_id
        cuid = fs.client_order_uid or ""

        try:
            st = self._call(self._order_state_call(), account_id=account_id, order_id=oid)
        except Exception as e:
            if self._is_not_found_error(e):
                self.log(f"[STATE] {self.format_instrument(figi)} order_id={oid} not found -> clearing local state")
                self.refresh_account_snapshot(account_id, [figi])
                if self._recover_missing_fill_from_snapshot(figi, getattr(fs, "order_side", ""), oid, cuid, getattr(fs, "active_order_reason", "")):
                    self.state.clear_order(figi)
                    self._reserved_rub_by_figi.pop(figi, None)
                    return
                self.journal_event(
                    "STATE_LOST",
                    figi,
                    side=str(getattr(fs, "order_side", "") or ""),
                    lots=None,
                    price=None,
                    order_id=oid,
                    client_uid=cuid,
                    status="NOT_FOUND",
                    reason="get_order_state_not_found",
                )
                self.state.clear_order(figi)
                self._reserved_rub_by_figi.pop(figi, None)
                return
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
                avg_price = float(self._to_float(ap))
            except Exception:
                avg_price = None
        fill_commission = self._calc_fixed_commission_rub(figi, lots_executed, avg_price)
        if fill_commission <= 0:
            fill_commission = self._extract_commission_from_order_state(st)

        side = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else str(getattr(fs, "order_side", "") or ""))
        order_reason = str(getattr(fs, "active_order_reason", "") or "")

        final_statuses = {
            "EXECUTION_REPORT_STATUS_FILL",
            "EXECUTION_REPORT_STATUS_REJECTED",
            "EXECUTION_REPORT_STATUS_CANCELLED",
        }

        if status in final_statuses:
            if status == "EXECUTION_REPORT_STATUS_FILL":
                self.log(
                    f"[FILL] {side} {self.format_instrument(figi)} lots={lots_executed} "
                    f"price={avg_price if avg_price is not None else 'N/A'} reason={order_reason or 'filled'}"
                )
                self.journal_event(
                    "FILL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason=order_reason or "filled",
                    meta={"commission_rub": float(fill_commission)},
                )

                if side == "BUY":
                    self.state.trades_today += 1
                    if fs.entry_time is None:
                        fs.entry_time = now()
                    if fs.entry_price is None and avg_price is not None:
                        fs.entry_price = float(avg_price)
                    fs.entry_commission_rub = float(getattr(fs, "entry_commission_rub", 0.0) or 0.0) + float(fill_commission)
                    self.notify_event(
                        "fill",
                        self.format_trade_fill_notification(
                            side=side,
                            figi=figi,
                            lots_executed=lots_executed,
                            avg_price=avg_price,
                            reason=order_reason,
                            commission_rub=float(fill_commission),
                        ),
                        throttle_sec=0,
                    )

                elif side == "SELL":
                    pnl_abs = None
                    pnl_pct = None
                    pnl_gross = None
                    total_commission = float(fill_commission)
                    try:
                        entry = fs.entry_price
                        if entry is not None and avg_price is not None:
                            lot_size = self._lot_size(figi)
                            qty_lots = float(lots_executed)
                            pnl_gross = (float(avg_price) - float(entry)) * float(lot_size) * qty_lots
                            total_commission += float(getattr(fs, "entry_commission_rub", 0.0) or 0.0)
                            pnl_abs = float(pnl_gross) - float(total_commission)
                            pnl_pct = (float(pnl_abs) / (float(entry) * float(lot_size) * qty_lots)) * 100.0
                            self.state.day_realized_pnl_rub += float(pnl_abs)
                            self.log(
                                f"[PNL] {self.format_instrument(figi)} gross={float(pnl_gross):+.2f} RUB "
                                f"commission={float(total_commission):.2f} RUB net={float(pnl_abs):+.2f} RUB"
                            )
                    except Exception:
                        pass

                    self.notify_event(
                        "fill",
                        self.format_trade_fill_notification(
                            side=side,
                            figi=figi,
                            lots_executed=lots_executed,
                            avg_price=avg_price,
                            reason=order_reason,
                            pnl_abs=pnl_abs,
                            pnl_pct=pnl_pct,
                            commission_rub=total_commission,
                        ),
                        throttle_sec=0,
                    )

                    fs.entry_price = None
                    fs.entry_time = None
                    fs.entry_commission_rub = 0.0

            elif status == "EXECUTION_REPORT_STATUS_CANCELLED":
                self.journal_event(
                    "CANCEL",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="cancelled_by_api",
                )
                self.notify_event("cancel", f"[CANCELLED] {self._ticker_for_figi(figi) or figi}", throttle_sec=0)

            elif status == "EXECUTION_REPORT_STATUS_REJECTED":
                self.journal_event(
                    "REJECT",
                    figi,
                    side=side,
                    lots=lots_executed,
                    price=avg_price,
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="rejected",
                )
                self.notify_event(
                    "reject",
                    f"[REJECT] {self._ticker_for_figi(figi) or figi} | status={status}",
                    throttle_sec=60,
                )

            self.state.clear_order(figi)
            self._reserved_rub_by_figi.pop(figi, None)

    def reconcile_recent_fills(self, account_id: str, figis: List[str], lookback_minutes: int = 180) -> int:
        """
        Best-effort reconciliation through operations API:
        if execution happened but per-order polling missed final status,
        backfill FILL events into journal.
        """
        from_ = now() - timedelta(minutes=int(max(5, lookback_minutes)))
        to_ = now()
        reconciled = 0
        figi_set = set(figis)

        try:
            resp = self._call(self._operations_call(), account_id=account_id, from_=from_, to=to_)
            operations = getattr(resp, "operations", []) or []
        except Exception as e:
            self.log(f"[WARN] reconcile_recent_fills failed (operations call): {e}")
            return 0

        for op in operations:
            figi = str(getattr(op, "figi", "") or "")
            if figi not in figi_set:
                continue

            op_type = str(getattr(op, "operation_type", "") or "").upper()
            if ("BUY" not in op_type) and ("SELL" not in op_type):
                continue
            side = "BUY" if "BUY" in op_type else "SELL"

            qty_raw = getattr(op, "quantity", 0)
            qty = int(abs(self._to_float(qty_raw)))
            if qty <= 0:
                continue

            order_id = str(getattr(op, "parent_operation_id", "") or "")
            if not order_id:
                order_id = str(getattr(op, "id", "") or "")
            if not order_id:
                continue

            if order_id in self._journaled_fill_order_ids:
                continue

            price_obj = getattr(op, "price", None)
            px = float(self._to_float(price_obj)) if price_obj is not None else None
            self.journal_event(
                "FILL",
                figi,
                side=side,
                lots=qty,
                price=px,
                order_id=order_id,
                client_uid="",
                status="RECONCILED",
                reason="reconciled_from_operations_api",
                meta={"op_type": op_type, "source": "operations_api"},
            )
            self.log(f"[RECONCILE] FILL restored {self.format_instrument(figi)} side={side} lots={qty} order_id={order_id}")
            reconciled += 1

        return reconciled

    # ---------- flatten ----------
    def flatten_if_needed(self, account_id: str, schedule_cfg: dict):
        ts = now()
        if not self.flatten_due(ts, schedule_cfg):
            return

        for figi in list(self.state.figi.keys()):
            fs = self.state.get(figi)

            if fs.active_order_id:
                self.cancel_active_order(account_id, figi, reason="flatten_cancel")

            if int(fs.position_lots) > 0:
                last = self.get_last_price(figi)
                if last is None:
                    continue
                self.place_limit_sell_to_close(account_id, figi, price=float(last))

    # ---------- day metric ----------
    def calc_day_risk_metric(self, figis: List[str]) -> float:
        """
        Day risk metric for lock logic:
          realized PnL from today's closed SELL fills
          + unrealized PnL on currently open positions.
        Uses only bot strategy state and ignores unrelated account cashflows.
        """
        realized = float(getattr(self.state, "day_realized_pnl_rub", 0.0) or 0.0)
        unrealized = 0.0

        for figi in figis:
            fs = self.state.get(figi)
            lots = int(getattr(fs, "position_lots", 0) or 0)
            entry = getattr(fs, "entry_price", None)
            if lots <= 0 or entry is None:
                continue

            last = self.get_last_price(figi)
            if last is None:
                continue

            lot_size = self._lot_size(figi)
            unrealized += (float(last) - float(entry)) * float(lot_size) * float(lots)

        return float(realized + unrealized)
