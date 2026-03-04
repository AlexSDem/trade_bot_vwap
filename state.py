from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from typing import Optional, Dict


@dataclass
class FigiState:
    active_order_id: Optional[str] = None       # биржевой order_id (ответ API)
    client_order_uid: Optional[str] = None      # наш idempotency key

    # NEW: чтобы понимать что за ордер висит и когда поставили (TTL)
    order_side: Optional[str] = None            # "BUY" / "SELL"
    order_placed_ts: Optional[datetime] = None  # when order was placed (UTC)
    active_order_reason: Optional[str] = None   # signal/trigger text for current order

    # position_lots хранится в ЛОТАХ (не в штуках)
    position_lots: int = 0

    # Для стратегии (тейк/стоп и т.п.)
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None
    entry_commission_rub: float = 0.0


@dataclass
class BotState:
    figi: Dict[str, FigiState] = field(default_factory=dict)
    trades_today: int = 0
    day_realized_pnl_rub: float = 0.0
    current_day: Optional[str] = None  # YYYY-MM-DD (UTC)

    def get(self, figi: str) -> FigiState:
        if figi not in self.figi:
            self.figi[figi] = FigiState()
        return self.figi[figi]

    def has_open_position(self, figi: str) -> bool:
        fs = self.figi.get(figi)
        return bool(fs and int(fs.position_lots) > 0)

    def has_active_order(self, figi: str) -> bool:
        fs = self.figi.get(figi)
        return bool(fs and fs.active_order_id)

    def open_positions_count(self) -> int:
        return sum(1 for fs in self.figi.values() if int(fs.position_lots) > 0)

    def clear_entry(self, figi: str):
        fs = self.get(figi)
        fs.entry_price = None
        fs.entry_time = None
        fs.entry_commission_rub = 0.0

    # NEW: clearing order meta in one place
    def clear_order(self, figi: str):
        fs = self.get(figi)
        fs.active_order_id = None
        fs.client_order_uid = None
        fs.order_side = None
        fs.order_placed_ts = None
        fs.active_order_reason = None

    def reset_day(self, day_key: str):
        self.current_day = day_key
        self.trades_today = 0
        self.day_realized_pnl_rub = 0.0
        # entry_* не трогаем — это состояние позиции, не дня

    def touch_day(self, day_key: str):
        if self.current_day != day_key:
            self.reset_day(day_key)

    @staticmethod
    def _dt_to_iso(v: Optional[datetime]) -> Optional[str]:
        if v is None:
            return None
        return v.isoformat()

    @staticmethod
    def _dt_from_iso(v: Optional[str]) -> Optional[datetime]:
        if not v:
            return None
        try:
            return datetime.fromisoformat(v)
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trades_today": int(self.trades_today),
            "day_realized_pnl_rub": float(self.day_realized_pnl_rub),
            "current_day": self.current_day,
            "figi": {
                figi: {
                    "active_order_id": fs.active_order_id,
                    "client_order_uid": fs.client_order_uid,
                    "order_side": fs.order_side,
                    "order_placed_ts": self._dt_to_iso(fs.order_placed_ts),
                    "active_order_reason": fs.active_order_reason,
                    "position_lots": int(fs.position_lots),
                    "entry_price": fs.entry_price,
                    "entry_time": self._dt_to_iso(fs.entry_time),
                    "entry_commission_rub": float(fs.entry_commission_rub),
                }
                for figi, fs in self.figi.items()
            },
        }

    def load_dict(self, payload: Dict[str, Any]):
        self.trades_today = int(payload.get("trades_today", 0) or 0)
        self.day_realized_pnl_rub = float(payload.get("day_realized_pnl_rub", 0.0) or 0.0)
        self.current_day = payload.get("current_day")

        figi_map = payload.get("figi", {}) or {}
        for figi, obj in figi_map.items():
            fs = self.get(figi)
            fs.active_order_id = obj.get("active_order_id")
            fs.client_order_uid = obj.get("client_order_uid")
            fs.order_side = obj.get("order_side")
            fs.order_placed_ts = self._dt_from_iso(obj.get("order_placed_ts"))
            fs.active_order_reason = obj.get("active_order_reason")
            fs.position_lots = int(obj.get("position_lots", 0) or 0)
            fs.entry_price = obj.get("entry_price")
            fs.entry_time = self._dt_from_iso(obj.get("entry_time"))
            fs.entry_commission_rub = float(obj.get("entry_commission_rub", 0.0) or 0.0)
