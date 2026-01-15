import os
import time
import yaml

from tinkoff.invest import Client
from tinkoff.invest.utils import now

from strategy import Strategy
from risk import RiskManager
from broker import Broker


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_token() -> str:
    token = os.environ.get("INVEST_TOKEN")
    if not token:
        raise RuntimeError(
            "Не задан INVEST_TOKEN. В PowerShell выполни:\n"
            '  setx INVEST_TOKEN "ТВОЙ_ТОКЕН"\n'
            "Затем открой новое окно PowerShell и запусти снова."
        )
    return token


def main():
    cfg = load_config()
    token = get_token()

    strategy = Strategy(cfg["strategy"])
    risk = RiskManager(cfg["risk"])

    sleep_sec = float(cfg.get("runtime", {}).get("sleep_sec", 55))
    error_sleep_sec = float(cfg.get("runtime", {}).get("error_sleep_sec", 10))

    with Client(token) as client:
        broker = Broker(client, cfg["broker"])

        account_id = broker.pick_account_id()
        broker.log(f"[INFO] Account: {account_id} (sandbox={cfg['broker'].get('use_sandbox', True)})")

        figis = broker.pick_tradeable_figis(cfg["universe"], max_lot_cost=cfg["risk"]["max_lot_cost_rub"])
        broker.log(f"[INFO] Tradeable FIGIs: {figis}")

        if not figis:
            broker.log("[ERROR] Не нашёл подходящих инструментов под max_lot_cost_rub. Увеличь лимит или измени tickers.")
            return

        while True:
            try:
                ts = now()

                # Если не торговое время — только flatten при необходимости и ждём
                if not broker.is_trading_time(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    time.sleep(min(10, sleep_sec))
                    continue

                # Если пора закрываться — закрываемся и ждём
                if broker.flatten_due(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    time.sleep(min(10, sleep_sec))
                    continue

                # Проверка дневного лимита
                if risk.day_locked():
                    time.sleep(30)
                    continue

                entries_allowed = broker.new_entries_allowed(ts, cfg["schedule"])

                for figi in figis:
                    # синхронизируем состояние по позициям/заявкам
                    broker.sync_state(account_id, figi)

                    # получаем свечи
                    candles = broker.get_last_candles_1m(figi, lookback_minutes=cfg["strategy"]["lookback_minutes"])
                    if candles is None or len(candles) < 30:
                        continue

                    signal = strategy.make_signal(figi, candles, broker.state)
                    # signal: {"action": "BUY"/"SELL"/"HOLD", "price": float, "reason": str}

                    # Если новые входы запрещены — игнорируем BUY (но выходы разрешаем)
                    if signal["action"] == "BUY" and not entries_allowed:
                        continue

                    # риск-фильтры на вход
                    if signal["action"] == "BUY":
                        if not risk.allow_new_trade(broker.state, account_id, figi):
                            continue
                        ok = broker.place_limit_buy(account_id, figi, signal["price"])
                        if ok:
                            broker.log(f"[SIGNAL] BUY {figi} @ {signal['price']} | {signal['reason']}")

                    elif signal["action"] == "SELL":
                        # закрытие позиции (тейк/стоп/тайм-стоп)
                        ok = broker.place_limit_sell_to_close(account_id, figi, signal["price"])
                        if ok:
                            broker.log(f"[SIGNAL] SELL {figi} @ {signal['price']} | {signal['reason']}")

                # Дневной "предохранитель": используем cashflow (защитный тормоз)
                cashflow = broker.calc_day_cashflow(account_id)
                risk.update_day_pnl(cashflow)

                time.sleep(sleep_sec)

            except KeyboardInterrupt:
                broker.log("[INFO] Stopped by user (Ctrl+C). Trying to flatten...")
                try:
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                except Exception as e:
                    broker.log(f"[WARN] Flatten on exit failed: {e}")
                break

            except Exception as e:
                # Не падаем на временных ошибках
                try:
                    broker.log(f"[ERROR] Main loop error: {e}")
                except Exception:
                    print("Main loop error:", e)
                time.sleep(error_sleep_sec)


if __name__ == "__main__":
    main()
