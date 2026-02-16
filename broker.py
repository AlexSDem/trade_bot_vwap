import os
import uuid
import math
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
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

    def __init__(self, client: Client, cfg: dict, notifier=None):
        self.client = client
        self.cfg = cfg
        self.state = BotState()
        self.notifier = notifier

        self._last_low_cash_warn: Dict[str, float] = {}
        self._reserved_rub_by_figi: Dict[str, float] = {}

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

        self._retry_tries = int(cfg.get("retry_tries", 3))
        self._retry_sleep_min = float(cfg.get("retry_sleep_min", 1.0))
        self._retry_sleep_max = float(cfg.get("retry_sleep_max", 10.0))

        # NEW: how close to last we want to place limit orders (ticks)
        self.buy_aggressive_ticks = int(cfg.get("buy_aggressive_ticks", 1))
        self.sell_aggressive_ticks = int(cfg.get("sell_aggressive_ticks", 1))

        self._figi_info: Dict[str, InstrumentInfo] = {}
        self.last_cash_rub: float = 0.0

        self.journal = TradeJournal(cfg.get("trades_csv", "logs/trades.csv"))

    # ---------- logging ----------
    def log(self, msg: str):
        self.logger.info(msg)
        print(msg)

    def notify(self, text: str, throttle_sec: float = 0.0):
        if not self.notifier:
            return
        try:
            self.notifier.send(text, throttle_sec=throttle_sec)
        except Exception:
            pass

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

    # ---------- day helpers ----------
    def _today_key(self) -> str:
        return datetime.now(tz=ZoneInfo("UTC")).date().isoformat()

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
            active_by_figi: Dict[str, str] = {}
            for o in orders:
                f = getattr(o, "figi", "")
                if f in figi_set and f not in active_by_figi:
                    active_by_figi[f] = getattr(o, "order_id", "")

            for f in figi_set:
                fs = self.state.get(f)
                fs.active_order_id = active_by_figi.get(f) or None
                if fs.active_order_id is None:
                    self.state.clear_order(f)
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

    def cancel_active_order(self, account_id: str, figi: str, reason: str = "cancel_active_order") -> bool:
        fs = self.state.get(figi)
        if not fs.active_order_id:
            return True

        oid = fs.active_order_id
        cuid = fs.client_order_uid or ""

        try:
            self._call(self._order_cancel_call(), account_id=account_id, order_id=oid)
            self.log(f"[CANCEL] {self.format_instrument(figi)} order_id={oid}")
            self.notify(f"[CANCEL] {self._ticker_for_figi(figi) or figi} order_id={oid}", throttle_sec=0)

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
                self.state.clear_order(figi)
                self._reserved_rub_by_figi.pop(figi, None)
                return True
            else:
                self.log(f"[WARN] cancel_order failed: {e}")
                self.notify(f"[WARN] cancel failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)
                return False

    def place_limit_buy(self, account_id: str, figi: str, price: float, quantity_lots: int = 1) -> bool:
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
                self.notify(msg, throttle_sec=0)

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

            self._reserved_rub_by_figi[figi] = float(est_cost)

            inst = self.format_instrument(figi)
            cash2 = self.get_cached_cash_rub(account_id)
            free2 = self.get_free_cash_rub_estimate(account_id)
            self.log(
                f"[ORDER] BUY {inst} qty={int(quantity_lots)} price={price_f} "
                f"| cash≈{cash2:.2f} free≈{free2:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.notify(
                f"[ORDER] BUY {inst} qty={int(quantity_lots)} price={price_f} | free≈{free2:.2f} {self.currency.upper()}",
                throttle_sec=0,
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
                meta={"est_cost": est_cost},
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order BUY failed: {e}")
            self.notify(f"[WARN] BUY submit failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)
            self.state.clear_order(figi)
            self._reserved_rub_by_figi.pop(figi, None)
            return False

    def place_limit_sell_to_close(self, account_id: str, figi: str, price: float) -> bool:
        fs = self.state.get(figi)
        if int(fs.position_lots) <= 0:
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

            self._reserved_rub_by_figi.pop(figi, None)

            inst = self.format_instrument(figi)
            cash = self.get_cached_cash_rub(account_id)
            free = self.get_free_cash_rub_estimate(account_id)
            self.log(
                f"[ORDER] SELL {inst} qty={int(fs.position_lots)} price={price_f} "
                f"| cash≈{cash:.2f} free≈{free:.2f} {self.currency.upper()} (client_uid={client_uid})"
            )
            self.notify(
                f"[ORDER] SELL {inst} qty={int(fs.position_lots)} price={price_f} | cash≈{cash:.2f} {self.currency.upper()}",
                throttle_sec=0,
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
            )

            return True
        except Exception as e:
            self.log(f"[WARN] post_order SELL failed: {e}")
            self.notify(f"[WARN] SELL submit failed: {self._ticker_for_figi(figi) or figi} | {e}", throttle_sec=120)
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
        self.notify(f"[EXPIRE] {inst} age={age:.0f}s -> cancel", throttle_sec=0)

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

        side = "BUY" if "BUY" in direction else ("SELL" if "SELL" in direction else str(getattr(fs, "order_side", "") or ""))

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
                    order_id=oid,
                    client_uid=cuid,
                    status=status,
                    reason="filled",
                )

                if side == "BUY":
                    self.state.trades_today += 1

                self.notify(
                    f"[FILL] {side} {self._ticker_for_figi(figi) or figi} lots={lots_executed} price={avg_price}",
                    throttle_sec=0,
                )

                if side == "BUY":
                    if fs.entry_time is None:
                        fs.entry_time = now()
                    if fs.entry_price is None and avg_price is not None:
                        fs.entry_price = float(avg_price)

                elif side == "SELL":
                    try:
                        entry = fs.entry_price
                        if entry is not None and avg_price is not None:
                            lot_size = self._lot_size(figi)
                            qty_lots = float(lots_executed)
                            pnl_abs = (float(avg_price) - float(entry)) * float(lot_size) * qty_lots
                            pnl_pct = (float(avg_price) / float(entry) - 1.0) * 100.0
                            self.state.day_realized_pnl_rub += float(pnl_abs)
                            self.notify(
                                f"[PNL] {self._ticker_for_figi(figi) or figi} "
                                f"{pnl_abs:+.2f} RUB ({pnl_pct:+.2f}%) | entry={float(entry):.4f} exit={float(avg_price):.4f}",
                                throttle_sec=0,
                            )
                    except Exception:
                        pass

                    fs.entry_price = None
                    fs.entry_time = None

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
                self.notify(f"[CANCELLED] {self._ticker_for_figi(figi) or figi}", throttle_sec=0)

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
                self.notify(f"[REJECT] {self._ticker_for_figi(figi) or figi} | status={status}", throttle_sec=60)

            self.state.clear_order(figi)
            self._reserved_rub_by_figi.pop(figi, None)

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
