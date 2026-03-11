import os
import time
import yaml
from datetime import datetime, timezone
from pathlib import Path
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


def save_daily_report(report_text: str, report_day_local, cfg: dict) -> str:
    reports_dir = cfg.get("runtime", {}).get("reports_dir", "logs/reports")
    os.makedirs(reports_dir, exist_ok=True)
    out_path = Path(reports_dir) / f"{report_day_local.isoformat()}.txt"
    generated_at = datetime.now(timezone.utc).isoformat()
    payload = f"Generated at (UTC): {generated_at}\n\n{report_text}\n"
    out_path.write_text(payload, encoding="utf-8")
    return str(out_path)


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
    midday_report_time = cfg.get("runtime", {}).get("midday_report_time", "15:00")
    schedule_tz = ZoneInfo(cfg["schedule"]["tz"])
    midday_report_hhmm = None
    reconcile_sec = float(cfg.get("runtime", {}).get("reconcile_sec", 600))

    # NEW: order TTL seconds (cancel if not filled)
    order_ttl_sec = int(cfg.get("runtime", {}).get("order_ttl_sec", 120))  # 2 minutes default
    order_reprice_sec = int(cfg.get("runtime", {}).get("order_reprice_sec", 90))

    with Client(token) as client:
        broker = Broker(
            client,
            cfg["broker"],
            notifier=notifier,
            notify_cfg=cfg.get("telegram", {}),
            trading_tz=cfg["schedule"]["tz"],
        )
        midday_report_hhmm = broker._parse_hhmm(str(midday_report_time))

        account_id = broker.pick_account_id()
        broker.log(f"[INFO] Account: {account_id} (sandbox={cfg['broker'].get('use_sandbox', True)})")

        broker.notify_event(
            "startup",
            (
                "Trade bot started\n"
                f"Account: {account_id}\n"
                f"Sandbox: {cfg['broker'].get('use_sandbox', True)}\n"
                f"Timezone: {cfg['schedule']['tz']}\n"
                f"Session: {cfg['schedule']['start_trade']} - {cfg['schedule']['flatten_time']}\n"
                f"Max lot cost: {cfg['risk']['max_lot_cost_rub']} RUB\n"
                f"Max day loss: {cfg['risk']['max_day_loss_rub']} RUB\n"
                f"Max trades/day: {cfg['risk']['max_trades_per_day']}"
            ),
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

        broker.load_runtime_state(account_id)
        broker.refresh_account_snapshot(account_id, figis)
        broker.save_runtime_state()

        # portfolio snapshot on start
        try:
            txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (start)")
            broker.log(txt)
            broker.notify_event(
                "portfolio",
                broker.build_portfolio_status_telegram(account_id, figis, title="Portfolio snapshot (start)"),
                throttle_sec=0,
            )
        except Exception as e:
            broker.log(f"[WARN] Portfolio snapshot failed (start): {e}")

        last_hb = 0.0
        last_portfolio_push = 0.0
        last_reconcile = 0.0
        consecutive_errors = 0
        report_sent_for_day: str | None = None
        midday_report_sent_for_day: str | None = None

        while True:
            try:
                ts = now()
                ts_local = ts.astimezone(schedule_tz)
                day_key = ts_local.date().isoformat()
                risk.touch_day(day_key)

                # Midday report once per local day
                if ts_local.time() >= midday_report_hhmm and midday_report_sent_for_day != day_key:
                    try:
                        title = f"Промежуточный отчет {ts_local.strftime('%Y-%m-%d %H:%M %Z')}"
                        txt = broker.build_intraday_report_telegram(account_id, figis, title=title)
                        broker.log(txt)
                        broker.notify_event("daily_report", txt, throttle_sec=0)
                    except Exception as e:
                        broker.log(f"[WARN] Midday report failed: {e}")
                    midday_report_sent_for_day = day_key

                # Heartbeat
                if time.time() - last_hb >= heartbeat_sec:
                    broker.log(f"[HB] alive | utc={ts.isoformat()}")
                    last_hb = time.time()

                # Portfolio snapshot every N seconds
                if time.time() - last_portfolio_push >= portfolio_sec:
                    try:
                        txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot")
                        broker.log(txt)
                        broker.notify_event(
                            "portfolio",
                            broker.build_portfolio_status_telegram(account_id, figis, title="Portfolio snapshot"),
                            throttle_sec=0,
                        )
                    except Exception as e:
                        broker.log(f"[WARN] Portfolio snapshot failed: {e}")
                    last_portfolio_push = time.time()

                # Periodic reconciliation with operations API
                if time.time() - last_reconcile >= reconcile_sec:
                    try:
                        restored = broker.reconcile_recent_fills(account_id, figis, lookback_minutes=180)
                        if restored > 0:
                            broker.log(f"[INFO] Reconcile restored fills: {restored}")
                    except Exception as e:
                        broker.log(f"[WARN] Reconcile failed: {e}")
                    last_reconcile = time.time()

                # Outside trading window
                if not broker.is_trading_time(ts, cfg["schedule"]):
                    broker.flatten_if_needed(account_id, cfg["schedule"])

                    # End of day report + end portfolio
                    if broker.flatten_due(ts, cfg["schedule"]):
                        day_key = ts_local.date().isoformat()
                        if report_sent_for_day != day_key:
                            try:
                                df = load_trades(cfg["broker"].get("trades_csv", "logs/trades.csv"))
                                report_day = ts_local.date()
                                report = build_report(df, report_day, tz_name=cfg["schedule"]["tz"])
                                report_path = save_daily_report(report, report_day, cfg)
                                broker.log(report)
                                broker.log(f"[INFO] Daily report saved: {report_path}")
                                broker.notify_event(
                                    "daily_report",
                                    f"Daily report\nFile: {report_path}\n\n{report}",
                                    throttle_sec=0,
                                )

                                try:
                                    txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (end)")
                                    broker.log(txt)
                                    broker.notify_event(
                                        "portfolio",
                                        broker.build_portfolio_status_telegram(
                                            account_id, figis, title="Portfolio snapshot (end)"
                                        ),
                                        throttle_sec=0,
                                    )
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

                    day_key = ts_local.date().isoformat()
                    if report_sent_for_day != day_key:
                        try:
                            df = load_trades(cfg["broker"].get("trades_csv", "logs/trades.csv"))
                            report_day = ts_local.date()
                            report = build_report(df, report_day, tz_name=cfg["schedule"]["tz"])
                            report_path = save_daily_report(report, report_day, cfg)
                            broker.log(report)
                            broker.log(f"[INFO] Daily report saved: {report_path}")
                            broker.notify_event(
                                "daily_report",
                                f"Daily report\nFile: {report_path}\n\n{report}",
                                throttle_sec=0,
                            )

                            try:
                                txt = broker.build_portfolio_status(account_id, figis, title="Portfolio snapshot (end)")
                                broker.log(txt)
                                broker.notify_event(
                                    "portfolio",
                                    broker.build_portfolio_status_telegram(account_id, figis, title="Portfolio snapshot (end)"),
                                    throttle_sec=0,
                                )
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
                    # 0) try to read final status first
                    broker.poll_order_updates(account_id, figi)

                    # 1) move stale working orders closer to market before hard TTL expiry
                    broker.reprice_stale_order(account_id, figi, reprice_sec=order_reprice_sec)

                    # 2) hard stop for too-old orders
                    broker.expire_stale_orders(account_id, figi, ttl_sec=order_ttl_sec)

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
                        if price is None:
                            continue
                        if limit_price is None:
                            limit_price = price

                        cash = broker.get_cached_cash_rub(account_id)
                        try:
                            free = broker.get_free_cash_rub_estimate(account_id)
                        except Exception:
                            free = cash

                        broker.log(
                            f"[SIGNAL] {action} {inst} last={price} limit={float(limit_price):.4f} "
                            f"| cash≈{cash:.2f} free≈{free:.2f} RUB | {reason}"
                        )
                        broker.notify_event(
                            "signal",
                            broker.format_signal_notification(
                                action=action,
                                figi=figi,
                                last=float(price),
                                limit_price=float(limit_price),
                                cash=float(cash),
                                free=float(free),
                                reason=str(reason),
                            ),
                            throttle_sec=0,
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
                        broker.place_limit_buy(
                            account_id,
                            figi,
                            signal.get("limit_price", signal["price"]),
                            reason=str(signal.get("reason", "") or ""),
                        )

                    elif action == "SELL":
                        broker.place_limit_sell_to_close(
                            account_id,
                            figi,
                            signal.get("limit_price", signal["price"]),
                            reason=str(signal.get("reason", "") or ""),
                        )

                # day protector
                day_metric = broker.calc_day_risk_metric(figis)
                risk.update_day_pnl(day_metric)
                broker.save_runtime_state()

                time.sleep(sleep_sec)
                consecutive_errors = 0

            except KeyboardInterrupt:
                broker.log("[INFO] Stopped by user (Ctrl+C). Trying to flatten...")
                broker.notify_event("service", "trade_bot stopped by user (Ctrl+C). Flattening...", throttle_sec=0)
                try:
                    broker.flatten_if_needed(account_id, cfg["schedule"])
                    broker.save_runtime_state()
                except Exception as e:
                    broker.log(f"[WARN] Flatten on exit failed: {e}")
                break

            except Exception as e:
                try:
                    broker.log(f"[ERROR] Main loop error: {e}")
                except Exception:
                    print("Main loop error:", e)
                try:
                    broker.save_runtime_state()
                except Exception:
                    pass

                consecutive_errors += 1
                broker.notify_event("error", f"[ERROR] Main loop error: {e}", throttle_sec=120)

                if consecutive_errors >= int(cfg.get("runtime", {}).get("max_consecutive_errors", 8)):
                    broker.log("[ERROR] Too many consecutive errors. Stopping bot.")
                    broker.notify_event("error", "[FATAL] Too many consecutive errors. Stopping bot.", throttle_sec=0)
                    broker.save_runtime_state()
                    break
                time.sleep(error_sleep_sec)


if __name__ == "__main__":
    main()
