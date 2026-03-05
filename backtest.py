import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from risk import RiskManager
from state import BotState
from strategy import Strategy


@dataclass
class PendingOrder:
    side: str
    limit_price: float
    lots: int
    placed_ts: datetime
    reason: str


def parse_args():
    p = argparse.ArgumentParser(description="Offline backtest for intraday VWAP bot on 1m candle CSV files.")
    p.add_argument("--config", default="config.yaml", help="Path to config file")
    p.add_argument("--data-dir", default="data/history_1m", help="Folder with per-ticker CSV files")
    p.add_argument("--out-dir", default="logs/backtest", help="Where to save report and trades")
    p.add_argument("--initial-cash", type=float, default=100000.0, help="Initial cash in RUB")
    p.add_argument("--lot-size-default", type=int, default=1, help="Fallback lot size if not known")
    return p.parse_args()


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists() and path == "config.yaml":
        p = Path("config.yaml.example")
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def parse_hhmm(v: str):
    hh, mm = str(v).split(":")
    return int(hh), int(mm)


def is_trading_time(ts_local: datetime, schedule_cfg: dict) -> bool:
    sh, sm = parse_hhmm(schedule_cfg["start_trade"])
    fh, fm = parse_hhmm(schedule_cfg["flatten_time"])
    start = ts_local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    flatten = ts_local.replace(hour=fh, minute=fm, second=0, microsecond=0)
    return start <= ts_local < flatten


def new_entries_allowed(ts_local: datetime, schedule_cfg: dict) -> bool:
    sh, sm = parse_hhmm(schedule_cfg["start_trade"])
    eh, em = parse_hhmm(schedule_cfg["stop_new_entries"])
    start = ts_local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    stop_new = ts_local.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= ts_local < stop_new


def flatten_due(ts_local: datetime, schedule_cfg: dict) -> bool:
    fh, fm = parse_hhmm(schedule_cfg["flatten_time"])
    flatten = ts_local.replace(hour=fh, minute=fm, second=0, microsecond=0)
    return ts_local >= flatten


def load_history(data_dir: Path, tickers: List[str]) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        fp = data_dir / f"{ticker}.csv"
        if not fp.exists():
            continue
        df = pd.read_csv(fp)
        if df.empty:
            continue
        required = {"ts_utc", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            continue
        df["time"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
        df = df.dropna(subset=["time"]).copy()
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close", "volume"]).copy()
        df = df.sort_values("time").drop_duplicates(subset=["time"], keep="last")
        df["ticker"] = ticker
        out[ticker] = df[["time", "open", "high", "low", "close", "volume", "ticker"]].reset_index(drop=True)
    return out


def calc_max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = (equity - peak) / peak.replace(0, pd.NA)
    return float(dd.min(skipna=True) or 0.0)


def run_backtest(cfg: dict, data: Dict[str, pd.DataFrame], initial_cash: float, lot_size_default: int):
    schedule = cfg["schedule"]
    tz = ZoneInfo(schedule["tz"])
    strategy = Strategy(cfg["strategy"])
    risk = RiskManager(cfg["risk"])
    state = BotState()

    ttl_sec = int(cfg.get("runtime", {}).get("order_ttl_sec", 120))
    commission_rate = float(cfg.get("broker", {}).get("commission_pct", 0.04)) / 100.0
    buy_ticks = int(cfg.get("broker", {}).get("buy_aggressive_ticks", 1))
    sell_ticks = int(cfg.get("broker", {}).get("sell_aggressive_ticks", 1))
    lot_size_by_ticker = {t: int(lot_size_default) for t in data.keys()}

    timeline = sorted(set().union(*[set(df["time"].tolist()) for df in data.values()]))
    if not timeline:
        raise RuntimeError("No candle rows for backtest timeline")

    bars_by_ts: Dict[datetime, Dict[str, dict]] = {}
    for ticker, df in data.items():
        for r in df.itertuples(index=False):
            bars_by_ts.setdefault(r.time.to_pydatetime(), {})[ticker] = {
                "open": float(r.open),
                "high": float(r.high),
                "low": float(r.low),
                "close": float(r.close),
                "volume": float(r.volume),
            }

    buffers: Dict[str, List[dict]] = {t: [] for t in data.keys()}
    pending: Dict[str, Optional[PendingOrder]] = {t: None for t in data.keys()}
    cash = float(initial_cash)
    trades: List[dict] = []
    equity_rows: List[dict] = []

    def append_trade(event: str, ticker: str, ts: datetime, **kwargs):
        row = {"ts_utc": ts.isoformat(), "event": event, "ticker": ticker}
        row.update(kwargs)
        trades.append(row)

    def position_value(last_prices: Dict[str, float]) -> float:
        total = 0.0
        for t in data.keys():
            fs = state.get(t)
            if int(fs.position_lots) <= 0:
                continue
            px = last_prices.get(t)
            if px is None:
                continue
            total += float(fs.position_lots) * float(lot_size_by_ticker[t]) * float(px)
        return total

    last_prices: Dict[str, float] = {}
    for ts in timeline:
        ts_local = ts.astimezone(tz)
        day_key = ts_local.date().isoformat()
        state.touch_day(day_key)
        risk.touch_day(day_key)

        bars = bars_by_ts.get(ts, {})
        for t, b in bars.items():
            buffers[t].append({"time": ts, "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"], "volume": b["volume"]})
            last_prices[t] = b["close"]
            max_len = int(max(60, cfg["strategy"].get("lookback_minutes", 180) + 5))
            if len(buffers[t]) > max_len:
                buffers[t] = buffers[t][-max_len:]

        # 1) Pending order management: fill first, then expire by TTL.
        for t, po in pending.items():
            if po is None:
                continue
            bar = bars.get(t)
            if bar is None:
                continue
            fs = state.get(t)
            lot_size = lot_size_by_ticker[t]

            filled = False
            fill_price = po.limit_price
            if po.side == "BUY" and bar["low"] <= po.limit_price:
                cost = po.lots * lot_size * fill_price
                fee = cost * commission_rate
                if cash >= (cost + fee):
                    cash -= (cost + fee)
                    fs.position_lots += po.lots
                    fs.entry_price = fill_price
                    fs.entry_time = ts
                    fs.entry_commission_rub += fee
                    state.trades_today += 1
                    append_trade("FILL", t, ts, side="BUY", lots=po.lots, price=fill_price, commission_rub=fee, reason=po.reason)
                    filled = True
            elif po.side == "SELL" and bar["high"] >= po.limit_price and int(fs.position_lots) >= po.lots:
                proceeds = po.lots * lot_size * fill_price
                fee = proceeds * commission_rate
                cash += (proceeds - fee)
                entry_px = float(fs.entry_price) if fs.entry_price is not None else fill_price
                gross = (fill_price - entry_px) * lot_size * po.lots
                net = gross - fee - float(getattr(fs, "entry_commission_rub", 0.0) or 0.0)
                state.day_realized_pnl_rub += net
                fs.position_lots -= po.lots
                if fs.position_lots <= 0:
                    fs.position_lots = 0
                    fs.entry_price = None
                    fs.entry_time = None
                    fs.entry_commission_rub = 0.0
                append_trade(
                    "FILL",
                    t,
                    ts,
                    side="SELL",
                    lots=po.lots,
                    price=fill_price,
                    commission_rub=fee,
                    pnl_gross_rub=gross,
                    pnl_net_rub=net,
                    reason=po.reason,
                )
                filled = True

            if filled:
                pending[t] = None
                fs.active_order_id = None
                fs.order_side = None
                fs.order_placed_ts = None
                fs.active_order_reason = None
                continue

            age_sec = (ts - po.placed_ts).total_seconds()
            if age_sec >= ttl_sec:
                append_trade("EXPIRE", t, ts, side=po.side, lots=po.lots, price=po.limit_price, age_sec=age_sec, ttl_sec=ttl_sec)
                pending[t] = None
                fs.active_order_id = None
                fs.order_side = None
                fs.order_placed_ts = None
                fs.active_order_reason = None

        # 2) Flatten when outside trade session and flatten time reached.
        if not is_trading_time(ts_local, schedule) and flatten_due(ts_local, schedule):
            for t in data.keys():
                fs = state.get(t)
                if int(fs.position_lots) <= 0:
                    continue
                bar = bars.get(t)
                px = float(bar["close"]) if bar is not None else float(last_prices.get(t, 0.0))
                if px <= 0:
                    continue
                lot_size = lot_size_by_ticker[t]
                qty = int(fs.position_lots)
                proceeds = qty * lot_size * px
                fee = proceeds * commission_rate
                cash += (proceeds - fee)
                entry_px = float(fs.entry_price) if fs.entry_price is not None else px
                gross = (px - entry_px) * lot_size * qty
                net = gross - fee - float(getattr(fs, "entry_commission_rub", 0.0) or 0.0)
                state.day_realized_pnl_rub += net
                append_trade("FORCE_FLATTEN", t, ts, side="SELL", lots=qty, price=px, pnl_net_rub=net)
                fs.position_lots = 0
                fs.entry_price = None
                fs.entry_time = None
                fs.entry_commission_rub = 0.0
                pending[t] = None
                fs.active_order_id = None
                fs.order_side = None
                fs.order_placed_ts = None
                fs.active_order_reason = None

        # 3) Signals and new orders.
        entries_allowed = new_entries_allowed(ts_local, schedule)
        if is_trading_time(ts_local, schedule) and not risk.day_locked():
            for t in data.keys():
                bar = bars.get(t)
                if bar is None:
                    continue
                if len(buffers[t]) < 30:
                    continue

                candles = pd.DataFrame(buffers[t])
                signal = strategy.make_signal(t, candles, state)
                action = signal.get("action", "HOLD")
                if action not in ("BUY", "SELL"):
                    continue

                fs = state.get(t)
                if action == "BUY":
                    if not entries_allowed:
                        continue
                    ok, _ = risk.allow_new_trade_reason(state, "backtest", t)
                    if not ok:
                        continue
                    if pending[t] is not None:
                        continue
                    limit_price = float(signal.get("limit_price", signal.get("price", bar["close"])))
                    # Approximate "aggressive" limit by moving one synthetic tick.
                    limit_price += buy_ticks * max(limit_price * 0.0001, 0.01)
                    po = PendingOrder(
                        side="BUY",
                        limit_price=float(limit_price),
                        lots=1,
                        placed_ts=ts,
                        reason=str(signal.get("reason", "")),
                    )
                    pending[t] = po
                    fs.active_order_id = f"bt-{t}-{int(ts.timestamp())}"
                    fs.order_side = "BUY"
                    fs.order_placed_ts = ts
                    fs.active_order_reason = po.reason
                    append_trade("SUBMIT", t, ts, side="BUY", lots=1, price=po.limit_price, reason=po.reason)

                elif action == "SELL":
                    if pending[t] is not None:
                        continue
                    if int(fs.position_lots) <= 0:
                        continue
                    limit_price = float(signal.get("limit_price", signal.get("price", bar["close"])))
                    limit_price -= sell_ticks * max(limit_price * 0.0001, 0.01)
                    qty = int(fs.position_lots)
                    po = PendingOrder(
                        side="SELL",
                        limit_price=float(limit_price),
                        lots=qty,
                        placed_ts=ts,
                        reason=str(signal.get("reason", "")),
                    )
                    pending[t] = po
                    fs.active_order_id = f"bt-{t}-{int(ts.timestamp())}"
                    fs.order_side = "SELL"
                    fs.order_placed_ts = ts
                    fs.active_order_reason = po.reason
                    append_trade("SUBMIT", t, ts, side="SELL", lots=qty, price=po.limit_price, reason=po.reason)

        # 4) Update day risk lock from realized pnl.
        risk.update_day_pnl(float(state.day_realized_pnl_rub))

        eq = cash + position_value(last_prices)
        equity_rows.append({"ts_utc": ts.isoformat(), "equity_rub": eq, "cash_rub": cash, "position_value_rub": eq - cash})

    equity_df = pd.DataFrame(equity_rows)
    trades_df = pd.DataFrame(trades)

    fills = trades_df[trades_df["event"] == "FILL"].copy() if not trades_df.empty else pd.DataFrame()
    sell_fills = fills[fills["side"] == "SELL"].copy() if not fills.empty else pd.DataFrame()
    wins = int((sell_fills.get("pnl_net_rub", pd.Series(dtype=float)) > 0).sum()) if not sell_fills.empty else 0
    total_sells = int(len(sell_fills))
    win_rate = (100.0 * wins / total_sells) if total_sells > 0 else 0.0

    final_equity = float(equity_df["equity_rub"].iloc[-1]) if not equity_df.empty else float(initial_cash)
    total_return_pct = ((final_equity / float(initial_cash)) - 1.0) * 100.0 if initial_cash > 0 else 0.0
    max_dd_pct = 100.0 * calc_max_drawdown(equity_df["equity_rub"]) if not equity_df.empty else 0.0

    summary = {
        "initial_cash_rub": float(initial_cash),
        "final_equity_rub": final_equity,
        "total_return_pct": total_return_pct,
        "max_drawdown_pct": max_dd_pct,
        "events_total": int(len(trades_df)),
        "submit_total": int((trades_df["event"] == "SUBMIT").sum()) if not trades_df.empty else 0,
        "fill_total": int((trades_df["event"] == "FILL").sum()) if not trades_df.empty else 0,
        "expire_total": int((trades_df["event"] == "EXPIRE").sum()) if not trades_df.empty else 0,
        "sell_fills_total": total_sells,
        "win_rate_pct": win_rate,
        "net_realized_pnl_rub": float(sell_fills.get("pnl_net_rub", pd.Series(dtype=float)).sum()) if not sell_fills.empty else 0.0,
    }
    return summary, trades_df, equity_df


def main():
    args = parse_args()
    cfg = load_config(args.config)

    tickers = list(cfg.get("universe", {}).get("tickers", []))
    if not tickers:
        raise RuntimeError("No tickers in config: universe.tickers")

    data_dir = Path(args.data_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = load_history(data_dir, tickers)
    if not data:
        raise RuntimeError(f"No usable history files in {data_dir}. Expected files like SBER.csv, GAZP.csv")

    summary, trades_df, equity_df = run_backtest(
        cfg=cfg,
        data=data,
        initial_cash=float(args.initial_cash),
        lot_size_default=int(args.lot_size_default),
    )

    summary_path = out_dir / "summary.json"
    trades_path = out_dir / "trades.csv"
    equity_path = out_dir / "equity.csv"
    txt_path = out_dir / "report.txt"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    if not trades_df.empty:
        trades_df.to_csv(trades_path, index=False)
    else:
        pd.DataFrame(columns=["ts_utc", "event", "ticker"]).to_csv(trades_path, index=False)
    equity_df.to_csv(equity_path, index=False)

    lines = [
        "Backtest Summary",
        "----------------",
        f"Initial cash: {summary['initial_cash_rub']:.2f} RUB",
        f"Final equity: {summary['final_equity_rub']:.2f} RUB",
        f"Return: {summary['total_return_pct']:+.2f}%",
        f"Max drawdown: {summary['max_drawdown_pct']:.2f}%",
        f"Events: total={summary['events_total']} submit={summary['submit_total']} fill={summary['fill_total']} expire={summary['expire_total']}",
        f"SELL fills: {summary['sell_fills_total']} | Win rate: {summary['win_rate_pct']:.1f}%",
        f"Net realized PnL: {summary['net_realized_pnl_rub']:+.2f} RUB",
        "",
        f"Saved: {summary_path}",
        f"Saved: {trades_path}",
        f"Saved: {equity_path}",
    ]
    txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
