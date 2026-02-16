import os
import time
from typing import Optional
import requests


class TelegramNotifier:
    def __init__(self, token: Optional[str], chat_id: Optional[str], enabled: bool = True):
        self.enabled = enabled and bool(token) and bool(chat_id)
        self.token = token
        self.chat_id = chat_id
        self._last_sent = 0.0

    def send(self, text: str, throttle_sec: float = 0.0):
        """
        throttle_sec: чтобы не спамить (например, на ошибках)
        """
        if not self.enabled:
            return

        now = time.time()
        if throttle_sec > 0 and (now - self._last_sent) < throttle_sec:
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }

        try:
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            self._last_sent = now
        except Exception:
            # уведомления не должны валить бота
            pass


def notifier_from_env(enabled: bool = True) -> TelegramNotifier:
    token = os.environ.get("TG_BOT_TOKEN")
    chat_id = os.environ.get("TG_CHAT_ID")
    return TelegramNotifier(token=token, chat_id=chat_id, enabled=enabled)
