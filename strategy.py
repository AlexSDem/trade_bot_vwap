import numpy as np
import pandas as pd

class Strategy:
    def __init__(self, cfg: dict):
        self.k = float(cfg.get("k_atr", 1.2))
        self.take_pct = float(cfg.get("take_profit_pct", 0.004))
        self.stop_pct = float(cfg.get("stop_loss_pct", 0.006))
        self.lookback = int(cfg.get("lookback_minutes", 180))

    @staticmethod
    def _atr(df: pd.DataFrame, n: int = 14) -> float:
        high = df["high"].values
        low = df["low"].values
        close = df["close"].values
        tr = np.maximum(high - low, np.maximum(np.abs(high - np.r_[close[0], close[:-1]]),
                                              np.abs(low - np.r_[close[0], close[:-1]])))
        if len(tr) < n + 1:
            return float(np.nan)
        return float(pd.Series(tr).rolling(n).mean().iloc[-1])

    @staticmethod
    def _vwap(df: pd.DataFrame) -> float:
        pv = (df["close"] * df["volume"]).sum()
        vv = df["volume"].sum()
        return float(pv / vv) if vv > 0 else float(df["close"].iloc[-1])

    def make_signal(self, candles: pd.DataFrame) -> dict:
        """
        candles columns: time, open, high, low, close, volume
        """
        df = candles.tail(self.lookback).copy()
        last = float(df["close"].iloc[-1])
        vwap = self._vwap(df)
        atr = self._atr(df, 14)

        # если ATR не посчитался — не торгуем
        if not np.isfinite(atr) or atr <= 0:
            return {"action": "HOLD", "price": last, "reason": "ATR not ready"}

        # mean reversion: цена существенно ниже VWAP
        buy_level = vwap - self.k * atr

        # Выходные сигналы (тейк/стоп) определяются в broker.py по факту открытой позиции,
        # но можно и здесь, если стратегия хранит entry_price.
        if last < buy_level:
            # лимитку ставим чуть лучше текущей (простая попытка не платить спред)
            limit_price = last  # можно last - 1 тик, если знаешь шаг цены
            return {"action": "BUY", "price": limit_price, "reason": f"last<{buy_level:.4f} VWAP={vwap:.4f} ATR={atr:.4f}"}

        return {"action": "HOLD", "price": last, "reason": "no edge"}
