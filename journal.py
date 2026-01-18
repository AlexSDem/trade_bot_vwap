import csv
import os
from datetime import datetime
from typing import Optional, Dict, Any


class TradeJournal:
    """
    Пишем события в CSV.
    Это не "аналитика", а простой журнал для контроля и последующего разбора в Excel.
    """

    def __init__(self, path: str = "logs/trades.csv"):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        self._ensure_header()

    def _ensure_header(self):
        if os.path.exists(self.path) and os.path.getsize(self.path) > 0:
            return

        with open(self.path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "ts_utc",
                "event",
                "figi",
                "ticker",
                "side",
                "lots",
                "price",
                "order_id",
                "client_uid",
                "status",
                "reason",
                "meta",
            ])

    def write(
        self,
        event: str,
        figi: str,
        ticker: str = "",
        side: str = "",
        lots: Optional[int] = None,
        price: Optional[float] = None,
        order_id: str = "",
        client_uid: str = "",
        status: str = "",
        reason: str = "",
        meta: Optional[Dict[str, Any]] = None,
    ):
        ts = datetime.utcnow().isoformat()
        meta_str = ""
        if meta:
            # простая сериализация без json-зависимостей
            meta_str = ";".join([f"{k}={v}" for k, v in meta.items()])

        with open(self.path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                ts,
                event,
                figi,
                ticker,
                side,
                "" if lots is None else lots,
                "" if price is None else f"{price:.6f}",
                order_id,
                client_uid,
                status,
                reason,
                meta_str,
            ])
