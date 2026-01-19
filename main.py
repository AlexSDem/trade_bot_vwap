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
            "Не задан INVEST_TOKEN.\n"
            "В PowerShell выполни:\n"
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

        # >>> NEW: ensure sandbox cash
        if cfg["broker"].get("use_sandbox", True):
            min_cash = float(cfg["broker"].get("min_sandbox_cash_rub", 12000))
            broker.ensure_sandbox_cash(account_id, min_cash_rub=min_cash)

        figis = broker.pick_tradeable_figis(cfg["universe"], max_lot_cost=cfg["risk"]["max_lot_cost_rub"])
        broker.log(f"[INFO] Tradeable FIGIs: {figis}")

        if not figis:
            broker.log("[ERROR] Нет подходящих инструментов под max_lot_cost_rub. Увеличь лимит или измени tickers.")
            return

        last_hb = 0.0

        while True:
            try:
                ts = now()

                # Heartbeat раз в минуту
                if time.time() - last_hb >= 60:
                    broker.log(f"[HB] alive | utc={ts.isoformat()}")
                    last_hb = time.time()

                # Вне торгового окна — только закрываемся при необходимости
                if not broker.is_trading_time(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    time.sleep(min(10, sleep_sec))
                    continue

                # Если время закрываться — закрываемся
                if broker.flatten_due(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    time.sleep(min(10, sleep_sec))
                    continue

                # Дневной лок
                if risk.day_locked():
                    time.sleep(30)
                    continue

                entries_allowed = broker.new_entries_allowed(ts, cfg["schedule"])

                for figi in figis:
                    # 1) синхронизируем состояние
                    broker.sync_state(account_id, figi)

                    # 2) проверяем изменения статуса активной заявки
                    broker.poll_order_updates(account_id, figi)

                    # 3) свечи
                    candles = broker.get_last_candles_1m(figi, lookback_minutes=cfg["strategy"]["lookback_minutes"])
                    if candles is None or len(candles) < 30:
                        continue

                    # 4) сигнал
                    signal = strategy.make_signal(figi, candles, broker.state)
                    action = signal.get("action", "HOLD")

                    # журналируем сигнал
                    if action in ("BUY", "SELL"):
                        broker.journal_event(
                            "SIGNAL",
                            figi,
                            side=action,
                            lots=1,
                            price=signal.get("price"),
                            reason=signal.get("reason", ""),
                        )

                    # 5) исполнение
                    if action == "BUY":
                        if not entries_allowed:
                            continue
                        if not risk.allow_new_trade(broker.state, account_id, figi):
                            continue

                        ok = broker.place_limit_buy(account_id, figi, signal["price"])
                        if ok:
                            broker.log(f"[SIGNAL] BUY {figi} @ {signal['price']} | {signal.get('reason', '')}")

                    elif action == "SELL":
                        ok = broker.place_limit_sell_to_close(account_id, figi, signal["price"])
                        if ok:
                            broker.log(f"[SIGNAL] SELL {figi} @ {signal['price']} | {signal.get('reason', '')}")

                # дневной предохранитель
                day_metric = broker.calc_day_cashflow(account_id)
                risk.update_day_pnl(day_metric)

                time.sleep(sleep_sec)

            except KeyboardInterrupt:
                broker.log("[INFO] Stopped by user (Ctrl+C). Trying to flatten...")
                try:
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                except Exception as e:
                    broker.log(f"[WARN] Flatten on exit failed: {e}")
                break

            except Exception as e:
                try:
                    broker.log(f"[ERROR] Main loop error: {e}")
                except Exception:
                    print("Main loop error:", e)
                time.sleep(error_sleep_sec)


if __name__ == "__main__":
    main()
