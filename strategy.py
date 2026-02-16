import numpy as np
import pandas as pd
from datetime import timedelta


class Strategy:
    def __init__(self, cfg: dict):
        self.k = float(cfg.get("k_atr", 1.2))
        self.take_pct = float(cfg.get("take_profit_pct", 0.004))
        self.stop_pct = float(cfg.get("stop_loss_pct", 0.006))
        self.lookback = int(cfg.get("lookback_minutes", 180))
        self.time_stop_minutes = int(cfg.get("time_stop_minutes", 45))

        # Existing entry filters (from earlier improved version)
        self.min_edge_atr = float(cfg.get("min_edge_atr", 0.05))
        self.max_rebound_atr = float(cfg.get("max_rebound_atr", 0.25))

        # --- NEW: Time-stop behavior change (Variant A) ---
        # After time_stop_minutes:
        # 1) tighten stop-loss closer to entry
        # 2) allow "safe exit" when price comes back to VWAP / breakeven

        # Stop-loss after time_stop becomes: entry * (1 - time_stop_tighten_stop_loss_pct)
        # (must be smaller than normal stop_pct to tighten risk)
        self.time_stop_tighten_stop_loss_pct = float(cfg.get("time_stop_tighten_stop_loss_pct", 0.0025))  # 0.25%

        # Breakeven level after time_stop: entry * (1 + breakeven_pct)
        # small positive to cover spread/fees/slippage
        self.breakeven_pct = float(cfg.get("breakeven_pct", 0.0005))  # 0.05%

        # Require safe-exit only if time-stop already reached
        self.enable_time_stop_safe_exit = bool(cfg.get("enable_time_stop_safe_exit", True))

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> float:
        high = df["high"].values
        low = df["low"].values
        close = df["close"].values
        prev_close = np.r_[close[0], close[:-1]]
        tr = np.maximum(
            high - low,
            np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)),
        )
        if len(tr) < n + 1:
            return float("nan")
        return float(pd.Series(tr).rolling(n).mean().iloc[-1])

    @staticmethod
    def _vwap(df: pd.DataFrame) -> float:
        pv = (df["close"] * df["volume"]).sum()
        vv = df["volume"].sum()
        if vv <= 0:
            return float(df["close"].iloc[-1])
        return float(pv / vv)

    def make_signal(self, figi: str, candles: pd.DataFrame, state) -> dict:
        """
        Returns dict like:
          - action: BUY/SELL/HOLD
          - price: last close (for reporting)
          - limit_price: recommended LIMIT price (for BUY/SELL)
          - reason: string
        """
        df = candles.tail(self.lookback).copy()
        last = float(df["close"].iloc[-1])

        atr = self._atr(df, 14)
        if not np.isfinite(atr) or atr <= 0:
            return {"action": "HOLD", "price": last, "reason": "ATR not ready"}

        vwap = self._vwap(df)
        fs = state.get(figi)
        has_pos = int(getattr(fs, "position_lots", 0) or 0) > 0
        has_active_order = bool(getattr(fs, "active_order_id", None))

        # If an order is already working - do not generate new entry/exit signals.
        if has_active_order:
            return {"action": "HOLD", "price": last, "reason": "active_order_wait"}

        # =========================
        # EXIT LOGIC (SELL)
        # =========================
        if has_pos:
            # Ensure entry bookkeeping exists
            if fs.entry_price is None:
                fs.entry_price = last
            if fs.entry_time is None:
                fs.entry_time = df["time"].iloc[-1]

            entry = float(fs.entry_price)

            # Base levels
            take_level = max(entry * (1 + self.take_pct), vwap)
            stop_level = entry * (1 - self.stop_pct)

            # Time-based behavior change (Variant A):
            # After time_stop_minutes:
            # - tighten stop closer to entry
            # - allow safe-exit at VWAP / breakeven (optional)
            age = None
            if fs.entry_time is not None:
                age = df["time"].iloc[-1] - fs.entry_time

            time_mode_on = bool(age is not None and age >= timedelta(minutes=self.time_stop_minutes))

            if time_mode_on:
                tightened_stop = entry * (1 - float(self.time_stop_tighten_stop_loss_pct))
                # Tighten = move stop UP (towards entry), i.e. reduce allowed loss
                stop_level = max(stop_level, tightened_stop)

                if self.enable_time_stop_safe_exit:
                    breakeven_level = entry * (1 + float(self.breakeven_pct))
                    safe_exit_level = max(vwap, breakeven_level)

                    # If price returned to VWAP / breakeven after long hold -> exit safely
                    if last >= safe_exit_level:
                        return {
                            "action": "SELL",
                            "price": last,
                            "limit_price": last,
                            "reason": f"safe_exit_after_time last>={safe_exit_level:.4f} age={age}",
                        }

            # Normal exits
            if last >= take_level:
                return {
                    "action": "SELL",
                    "price": last,
                    "limit_price": last,
                    "reason": f"take_profit last>={take_level:.4f}",
                }

            if last <= stop_level:
                # Note: stop_level may be tightened after time_stop_minutes
                suffix = f" (tightened)" if time_mode_on else ""
                return {
                    "action": "SELL",
                    "price": last,
                    "limit_price": last,
                    "reason": f"stop_loss last<={stop_level:.4f}{suffix}",
                }

            if time_mode_on:
                return {"action": "HOLD", "price": last, "reason": f"in_position_time_mode age={age}"}

            return {"action": "HOLD", "price": last, "reason": "in_position"}

        # =========================
        # ENTRY LOGIC (BUY)
        # =========================
        buy_level = float(vwap - self.k * atr)

        # Edge in ATR units (how deep below buy_level are we)
        edge_atr = float((buy_level - last) / atr)

        # If signal is too shallow -> skip (noise)
        if edge_atr < self.min_edge_atr:
            return {"action": "HOLD", "price": last, "reason": f"no_edge edge_atr={edge_atr:.3f}"}

        if last < buy_level:
            # Recommended LIMIT at buy_level (not at last)
            limit_price = buy_level
            return {
                "action": "BUY",
                "price": last,
                "limit_price": float(limit_price),
                "reason": f"mean_reversion last<{buy_level:.4f} edge_atr={edge_atr:.3f}",
            }

        # If market already bounced significantly above the entry threshold,
        # explicitly mark it as a chase skip.
        rebound_atr = float((last - buy_level) / atr)
        if rebound_atr > self.max_rebound_atr:
            return {"action": "HOLD", "price": last, "reason": f"skip_chase rebound_atr={rebound_atr:.3f}"}

        return {"action": "HOLD", "price": last, "reason": "no_edge"}
