import argparse
from datetime import datetime, date, timezone
from pathlib import Path
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Daily report from logs/trades.csv")
    p.add_argument("--csv", default="logs/trades.csv", help="Path to trades csv")
    p.add_argument("--date", default=None, help="Date YYYY-MM-DD (UTC by default). If not set: today UTC.")
    p.add_argument("--tz", default="UTC", help="Timezone label for display only (default UTC)")
    p.add_argument("--out", default=None, help="Optional output file (txt)")
    return p.parse_args()


def to_date(d: str | None) -> date:
    if not d:
        return datetime.now(timezone.utc).date()
    return datetime.strptime(d, "%Y-%m-%d").date()


def load_trades(csv_path: str) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(path)
    if df.empty:
        return df

    # expected columns from TradeJournal:
    # ts_utc,event,figi,ticker,side,lots,price,order_id,client_uid,status,reason,meta
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    df["lots"] = pd.to_numeric(df.get("lots"), errors="coerce")
    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    df["ticker"] = df.get("ticker", "").fillna("")
    df["side"] = df.get("side", "").fillna("")
    df["event"] = df.get("event", "").fillna("")
    df["reason"] = df.get("reason", "").fillna("")
    df["status"] = df.get("status", "").fillna("")
    return df


def build_report(df: pd.DataFrame, day: date) -> str:
    if df.empty:
        return f"No data in trades.csv\n"

    # filter by UTC date of ts_utc
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start.replace(hour=23, minute=59, second=59)

    ddf = df[(df["ts_utc"] >= day_start) & (df["ts_utc"] <= day_end)].copy()
    if ddf.empty:
        return f"No events for {day.isoformat()} (UTC)\n"

    lines = []
    lines.append(f"Daily report for {day.isoformat()} (UTC)")
    lines.append("-" * 60)
    lines.append(f"Events total: {len(ddf)}")
    lines.append("")

    signals_cnt = int((ddf["event"] == "SIGNAL").sum())
    submit_cnt = int((ddf["event"] == "SUBMIT").sum())
    fill_cnt = int(ddf["event"].isin(["FILL", "PARTIAL_FILL"]).sum())
    cancel_cnt = int((ddf["event"] == "CANCEL").sum())
    reject_cnt = int((ddf["event"] == "REJECT").sum())
    expire_cnt = int((ddf["event"] == "EXPIRE").sum())

    lines.append("Summary:")
    lines.append(
        f"  signals={signals_cnt} | submits={submit_cnt} | fills={fill_cnt} "
        f"| cancel={cancel_cnt} | reject={reject_cnt} | expire={expire_cnt}"
    )
    lines.append("")

    # Order lifecycle summary (SUBMIT -> final)
    with_oid = ddf[ddf["order_id"].fillna("").astype(str).str.strip() != ""].copy()
    final_events = {"FILL", "PARTIAL_FILL", "CANCEL", "REJECT", "STATE_LOST", "EXPIRE"}
    submit_orders = set(with_oid.loc[with_oid["event"] == "SUBMIT", "order_id"].astype(str))
    final_orders = set(with_oid.loc[with_oid["event"].isin(final_events), "order_id"].astype(str))
    closed_orders = submit_orders.intersection(final_orders)
    stuck_orders = sorted(list(submit_orders - final_orders))
    conversion_pct = (100.0 * len(closed_orders) / len(submit_orders)) if submit_orders else 0.0

    lines.append("Order lifecycle:")
    lines.append(
        f"  submit_orders={len(submit_orders)} | finalized_orders={len(closed_orders)} "
        f"| stuck_submit_without_final={len(stuck_orders)} | submit_to_final={conversion_pct:.1f}%"
    )
    if stuck_orders:
        lines.append("  Stuck order_id (last 10):")
        for oid in stuck_orders[-10:]:
            lines.append(f"    - {oid}")
    lines.append("")

    # Event counts
    lines.append("Events:")
    vc = ddf["event"].value_counts()
    for k, v in vc.items():
        lines.append(f"  {k:12s}: {int(v)}")
    lines.append("")

    # Fills summary
    fills = ddf[ddf["event"].isin(["FILL", "PARTIAL_FILL"])].copy()
    if not fills.empty:
        lines.append("Fills by side:")
        v2 = fills["side"].value_counts()
        for k, v in v2.items():
            lines.append(f"  {k:5s}: {int(v)}")
        lines.append("")

        lines.append("Fills by ticker (count):")
        v3 = fills["ticker"].value_counts()
        for k, v in v3.items():
            lines.append(f"  {k:8s}: {int(v)}")
        lines.append("")

        # Estimate turnover (lots * price) - useful even without real PnL
        fills["turnover"] = fills["lots"].fillna(0) * fills["price"].fillna(0)
        turnover_total = fills["turnover"].sum()
        buy_turnover = fills.loc[fills["side"] == "BUY", "turnover"].sum()
        sell_turnover = fills.loc[fills["side"] == "SELL", "turnover"].sum()
        net_lots = fills.apply(
            lambda r: (r["lots"] if r["side"] == "BUY" else (-r["lots"] if r["side"] == "SELL" else 0)),
            axis=1,
        ).sum()

        lines.append(f"Turnover total (approx): {turnover_total:,.2f}")
        lines.append(f"Turnover BUY/SELL: {buy_turnover:,.2f} / {sell_turnover:,.2f}")
        lines.append(f"Net filled lots (BUY-SELL): {int(net_lots)}")
        lines.append("")
    else:
        lines.append("No fills today.")
        lines.append("")

    # Per ticker lifecycle
    tdf = ddf.copy()
    tdf["ticker_show"] = tdf["ticker"].where(tdf["ticker"].astype(str).str.strip() != "", tdf["figi"])
    lines.append("By ticker (signal/submit/fill/cancel/reject/expire):")
    grouped = tdf.groupby("ticker_show", dropna=False)
    for ticker, g in grouped:
        signals = int((g["event"] == "SIGNAL").sum())
        submits = int((g["event"] == "SUBMIT").sum())
        fills = int(g["event"].isin(["FILL", "PARTIAL_FILL"]).sum())
        cancels = int((g["event"] == "CANCEL").sum())
        rejects = int((g["event"] == "REJECT").sum())
        expires = int((g["event"] == "EXPIRE").sum())
        lines.append(
            f"  {str(ticker):10s}: signal={signals:3d} submit={submits:3d} "
            f"fill={fills:3d} cancel={cancels:3d} reject={rejects:3d} expire={expires:3d}"
        )
    lines.append("")

    # Rejections/cancels
    bad = ddf[ddf["event"].isin(["REJECT", "CANCEL"])].copy()
    if not bad.empty:
        lines.append("Reject/Cancel (last 10):")
        tail = bad.sort_values("ts_utc").tail(10)
        for _, r in tail.iterrows():
            ts = r["ts_utc"].strftime("%H:%M:%S")
            lines.append(f"  {ts} {r['event']:6s} {r['ticker']:6s} {r['side']:4s} status={r['status']} reason={r['reason']}")
        lines.append("")

    # Top reasons to understand behavior
    reasons = ddf["reason"].fillna("").astype(str).str.strip()
    reasons = reasons[reasons != ""]
    if not reasons.empty:
        lines.append("Top reasons:")
        rv = reasons.value_counts().head(10)
        for k, v in rv.items():
            lines.append(f"  {k:40s} {int(v)}")
        lines.append("")

    # Last 15 key events
    lines.append("Last 15 events:")
    tail = ddf.sort_values("ts_utc").tail(15)
    for _, r in tail.iterrows():
        ts = r["ts_utc"].strftime("%H:%M:%S")
        ticker = r["ticker"] if r["ticker"] else r["figi"]
        side = r["side"] if r["side"] else "-"
        price = f"{r['price']:.4f}" if pd.notna(r["price"]) else "-"
        lots = f"{int(r['lots'])}" if pd.notna(r["lots"]) else "-"
        lines.append(f"  {ts} {r['event']:12s} {ticker:10s} {side:4s} lots={lots:>3s} price={price:>8s} {r['reason']}")
    lines.append("")

    return "\n".join(lines)


def main():
    args = parse_args()
    day = to_date(args.date)
    df = load_trades(args.csv)
    report = build_report(df, day)

    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"Wrote report to {args.out}")
    else:
        print(report)


if __name__ == "__main__":
    main()
