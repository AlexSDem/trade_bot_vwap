import os
import time
import yaml
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from tinkoff.invest import Client
from tinkoff.invest.utils import now

from strategy import Strategy
from risk import RiskManager
from broker import Broker
from telegram_notifier import notifier_from_env
from report_day import load_trades, build_report


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

    notifier = notifier_from_env(enabled=bool(cfg.get("telegram", {}).get("enabled", True)))

    strategy = Strategy(cfg["strategy"])
    risk = RiskManager(cfg["risk"])

    sleep_sec = float(cfg.get("runtime", {}).get("sleep_sec", 55))
    error_sleep_sec = float(cfg.get("runtime", {}).get("error_sleep_sec", 10))
    heartbeat_sec = float(cfg.get("runtime", {}).get("heartbeat_sec", 300))
    portfolio_sec = float(cfg.get("runtime", {}).get("portfolio_sec", 1800))  # 30 min default

    # NEW: order TTL seconds (cancel if not filled)
    order_ttl_sec = int(cfg.get("runtime", {}).get("order_ttl_sec", 120))  # 2 minutes default

    with Client(token) as client:
        broker = Broker(client, cfg["broker"], notifier=notifier)

        account_id = broker.pick_account_id()
        broker.log(f"[INFO] Account: {account_id} (sandbox={cfg['broker'].get('use_sandbox', True)})")

        notifier.send(
            f"trade_bot started\naccount={account_id}\nsandbox={cfg['broker'].get('use_sandbox', True)}",
            throttle_sec=0,
        )

        # ensure sandbox cash
        if cfg["broker"].get("use_sandbox", True):
            min_cash = float(cfg["broker"].get("min_sandbox_cash_rub", 12000))
            broker.ensure_sandbox_cash(account_id, min_cash_rub=min_cash)

        figis = broker.pick_tradeable_figis(cfg["universe"], max_lot_cost=cfg["risk"]["max_lot_cost_rub"])
        broker.log(f"[INFO] Tradeable FIGIs: {figis}")

        if not figis:
            broker.log("[ERROR] Нет подходящих инструментов под max_lot_cost_rub. Увеличь лимит или измени tickers.")
            return

        # portfolio snapshot on start
        try:
            txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (start)")
            broker.log(txt)
            notifier.send(txt, throttle_sec=0)
        except Exception as e:
            broker.log(f"[WARN] Portfolio snapshot failed (start): {e}")

        last_hb = 0.0
        last_portfolio_push = 0.0
        consecutive_errors = 0
        report_sent_for_day: str | None = None

        while True:
            try:
                ts = now()
                day_key = ts.astimezone(ZoneInfo(cfg["schedule"]["tz"])).date().isoformat()
                risk.touch_day(day_key)

                # Heartbeat
                if time.time() - last_hb >= heartbeat_sec:
                    broker.log(f"[HB] alive | utc={ts.isoformat()}")
                    last_hb = time.time()

                # Portfolio snapshot every N seconds
                if time.time() - last_portfolio_push >= portfolio_sec:
                    try:
                        txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot")
                        broker.log(txt)
                        notifier.send(txt, throttle_sec=0)
                    except Exception as e:
                        broker.log(f"[WARN] Portfolio snapshot failed: {e}")
                    last_portfolio_push = time.time()

                # Outside trading window
                if not broker.is_trading_time(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])

                    # End of day report + end portfolio
                    if broker.flatten_due(ts, cfg["schedule"]):
                        day_key = datetime.now(timezone.utc).date().isoformat()
                        if report_sent_for_day != day_key:
                            try:
                                df = load_trades(cfg["broker"].get("trades_csv", "logs/trades.csv"))
                                report = build_report(df, datetime.now(timezone.utc).date())
                                broker.log(report)
                                notifier.send(report, throttle_sec=0)

                                try:
                                    txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (end)")
                                    broker.log(txt)
                                    notifier.send(txt, throttle_sec=0)
                                except Exception as e:
                                    broker.log(f"[WARN] Portfolio snapshot failed (end): {e}")

                                report_sent_for_day = day_key
                            except Exception as e:
                                broker.log(f"[WARN] Daily report generation failed: {e}")

                    time.sleep(min(10, sleep_sec))
                    continue

                # Flatten time
                if broker.flatten_due(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])

                    day_key = datetime.now(timezone.utc).date().isoformat()
                    if report_sent_for_day != day_key:
                        try:
                            df = load_trades(cfg["broker"].get("trades_csv", "logs/trades.csv"))
                            report = build_report(df, datetime.now(timezone.utc).date())
                            broker.log(report)
                            notifier.send(report, throttle_sec=0)

                            try:
                                txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (end)")
                                broker.log(txt)
                                notifier.send(txt, throttle_sec=0)
                            except Exception as e:
                                broker.log(f"[WARN] Portfolio snapshot failed (end): {e}")

                            report_sent_for_day = day_key
                        except Exception as e:
                            broker.log(f"[WARN] Daily report generation failed: {e}")

                    time.sleep(min(10, sleep_sec))
                    continue

                # Day lock
                if risk.day_locked():
                    time.sleep(30)
                    continue

                entries_allowed = broker.new_entries_allowed(ts, cfg["schedule"])

                # Snapshot once per loop
                broker.refresh_account_snapshot(account_id, figis)

                for figi in figis:
                    # 0) expire stale orders first (free slots, keep bot "simple flow")
                    broker.expire_stale_orders(account_id, figi, ttl_sec=order_ttl_sec)

                    # 1) order status updates
                    broker.poll_order_updates(account_id, figi)

                    # candles
                    candles = broker.get_last_candles_1m(figi, lookback_minutes=cfg["strategy"]["lookback_minutes"])
                    if candles is None or len(candles) < 30:
                        continue

                    # signal
                    signal = strategy.make_signal(figi, candles, broker.state)
                    action = signal.get("action", "HOLD")

                    # journal signals
                    if action in ("BUY", "SELL"):
                        price = signal.get("price")
                        limit_price = signal.get("limit_price", price)
                        reason = signal.get("reason", "")
                        inst = broker.format_instrument(figi)

                        cash = broker.get_cached_cash_rub(account_id)
                        try:
                            free = broker.get_free_cash_rub_estimate(account_id)
                        except Exception:
                            free = cash

                        broker.log(
                            f"[SIGNAL] {action} {inst} last={price} limit={float(limit_price):.4f} "
                            f"| cash≈{cash:.2f} free≈{free:.2f} RUB | {reason}"
                        )

                        broker.journal_event(
                            "SIGNAL",
                            figi,
                            side=action,
                            lots=1,
                            price=price,
                            reason=reason,
                            meta={"limit_price": float(limit_price) if limit_price is not None else None},
                        )

                    # execute (simple loop: signal -> order now)
                    if action == "BUY":
                        if not entries_allowed:
                            continue
                        if not risk.allow_new_trade(broker.state, account_id, figi):
                            continue
                        broker.place_limit_buy(account_id, figi, signal.get("limit_price", signal["price"]))

                    elif action == "SELL":
                        broker.place_limit_sell_to_close(account_id, figi, signal.get("limit_price", signal["price"]))

                # day protector
                day_metric = broker.calc_day_risk_metric(figis)
                risk.update_day_pnl(day_metric)

                time.sleep(sleep_sec)
                consecutive_errors = 0

            except KeyboardInterrupt:
                broker.log("[INFO] Stopped by user (Ctrl+C). Trying to flatten...")
                notifier.send("trade_bot stopped by user (Ctrl+C). Flattening...", throttle_sec=0)
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

                consecutive_errors += 1
                notifier.send(f"[ERROR] Main loop error: {e}", throttle_sec=120)

                if consecutive_errors >= int(cfg.get("runtime", {}).get("max_consecutive_errors", 8)):
                    broker.log("[ERROR] Too many consecutive errors. Stopping bot.")
                    notifier.send("[FATAL] Too many consecutive errors. Stopping bot.", throttle_sec=0)
                    break
                time.sleep(error_sleep_sec)


if __name__ == "__main__":
    main()
