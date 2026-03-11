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
        self.last_error: str = ""

    @staticmethod
    def _split_text(text: str, max_len: int = 3500) -> list[str]:
        if len(text) <= max_len:
            return [text]

        chunks: list[str] = []
        remaining = text
        while remaining:
            if len(remaining) <= max_len:
                chunks.append(remaining)
                break

            split_at = remaining.rfind("\n", 0, max_len)
            if split_at < max_len // 2:
                split_at = max_len
            chunks.append(remaining[:split_at].rstrip())
            remaining = remaining[split_at:].lstrip("\n")
        return chunks

    def send(self, text: str, throttle_sec: float = 0.0) -> bool:
        """
        throttle_sec: чтобы не спамить (например, на ошибках)
        """
        if not self.enabled:
            self.last_error = "telegram_disabled_or_missing_credentials"
            return False

        now = time.time()
        if throttle_sec > 0 and (now - self._last_sent) < throttle_sec:
            self.last_error = "telegram_throttled"
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        try:
            chunks = self._split_text(text)
            total = len(chunks)
            for idx, chunk in enumerate(chunks, start=1):
                prefix = f"[{idx}/{total}]\n" if total > 1 else ""
                payload = {
                    "chat_id": self.chat_id,
                    "text": f"{prefix}{chunk}",
                    "disable_web_page_preview": True,
                }
                r = requests.post(url, json=payload, timeout=10)
                r.raise_for_status()
            self._last_sent = now
            self.last_error = ""
            return True
        except Exception as e:
            # пусть caller решает, как логировать проблему
            resp = getattr(e, "response", None)
            if resp is not None:
                body = resp.text.strip()
                self.last_error = f"telegram_http_{resp.status_code}: {body[:500]}"
            else:
                self.last_error = str(e) or e.__class__.__name__
            return False


def notifier_from_env(enabled: bool = True) -> TelegramNotifier:
    token = os.environ.get("TG_BOT_TOKEN")
    chat_id = os.environ.get("TG_CHAT_ID")
    return TelegramNotifier(token=token, chat_id=chat_id, enabled=enabled)
