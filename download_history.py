import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import yaml
from tinkoff.invest import Client, CandleInterval, InstrumentIdType, RequestError
from tinkoff.invest.utils import quotation_to_decimal


def parse_args():
    p = argparse.ArgumentParser(description="Download 1m candles history for configured tickers.")
    p.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    p.add_argument("--days", type=int, default=30, help="Lookback period in days (default: 30)")
    p.add_argument("--out-dir", default="data/history_1m", help="Output folder for ticker CSV files")
    p.add_argument("--class-code", default=None, help="Override class code (e.g. TQBR)")
    p.add_argument("--sleep-sec", type=float, default=0.1, help="Pause between API requests")
    p.add_argument("--retry-tries", type=int, default=4, help="Retries for request errors")
    return p.parse_args()


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists() and path == "config.yaml":
        p = Path("config.yaml.example")
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_token() -> str:
    token = os.environ.get("INVEST_TOKEN")
    if not token:
        raise RuntimeError("INVEST_TOKEN is not set")
    return token


def q_to_float(q) -> float:
    return float(quotation_to_decimal(q))


def call_retry(fn, *args, tries: int = 4, **kwargs):
    delay = 1.0
    for i in range(1, tries + 1):
        try:
            return fn(*args, **kwargs)
        except RequestError:
            if i >= tries:
                raise
            time.sleep(delay)
            delay = min(8.0, delay * 2.0)


def resolve_tickers(client: Client, tickers: List[str], class_code: str, retry_tries: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for t in tickers:
        try:
            r = call_retry(
                client.instruments.share_by,
                id=t,
                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                class_code=class_code,
                tries=retry_tries,
            )
            out[t] = r.instrument.figi
            print(f"[OK] {t} -> {r.instrument.figi}")
        except Exception as e:
            print(f"[WARN] skip ticker {t}: {e}")
    return out


def fetch_candles_1m(
    client: Client,
    figi: str,
    dt_from: datetime,
    dt_to: datetime,
    retry_tries: int,
) -> List[Tuple[str, float, float, float, float, int]]:
    rows: List[Tuple[str, float, float, float, float, int]] = []
    try:
        candles = client.get_all_candles(
            figi=figi,
            from_=dt_from,
            to=dt_to,
            interval=CandleInterval.CANDLE_INTERVAL_1_MIN,
        )
        for c in candles:
            rows.append(
                (
                    c.time.astimezone(timezone.utc).isoformat(),
                    q_to_float(c.open),
                    q_to_float(c.high),
                    q_to_float(c.low),
                    q_to_float(c.close),
                    int(c.volume),
                )
            )
    except RequestError:
        # single retry wrapper for generator-based endpoint
        if retry_tries <= 1:
            raise
        time.sleep(1.0)
        return fetch_candles_1m(client, figi, dt_from, dt_to, retry_tries - 1)
    return rows


def save_ticker_csv(path: Path, rows: List[Tuple[str, float, float, float, float, int]], ticker: str, figi: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts_utc", "open", "high", "low", "close", "volume", "ticker", "figi"])
        for r in rows:
            w.writerow([r[0], f"{r[1]:.9f}", f"{r[2]:.9f}", f"{r[3]:.9f}", f"{r[4]:.9f}", r[5], ticker, figi])


def main():
    args = parse_args()
    cfg = load_config(args.config)
    token = get_token()

    tickers = list(cfg.get("universe", {}).get("tickers", []))
    if not tickers:
        raise RuntimeError("No tickers found in config (universe.tickers)")

    class_code = args.class_code or cfg.get("broker", {}).get("class_code", "TQBR")
    dt_to = datetime.now(timezone.utc)
    dt_from = dt_to - timedelta(days=int(args.days))

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "from_utc": dt_from.isoformat(),
        "to_utc": dt_to.isoformat(),
        "days": int(args.days),
        "class_code": class_code,
        "tickers": {},
    }

    with Client(token) as client:
        mapping = resolve_tickers(client, tickers, class_code=class_code, retry_tries=int(args.retry_tries))
        if not mapping:
            raise RuntimeError("No ticker->figi mappings resolved")

        for ticker, figi in mapping.items():
            rows = fetch_candles_1m(
                client=client,
                figi=figi,
                dt_from=dt_from,
                dt_to=dt_to,
                retry_tries=int(args.retry_tries),
            )
            csv_path = out_dir / f"{ticker}.csv"
            save_ticker_csv(csv_path, rows, ticker=ticker, figi=figi)
            summary["tickers"][ticker] = {
                "figi": figi,
                "rows": len(rows),
                "path": str(csv_path),
            }
            print(f"[SAVE] {ticker}: {len(rows)} rows -> {csv_path}")
            time.sleep(float(args.sleep_sec))

    manifest = out_dir / "manifest.json"
    manifest.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[DONE] manifest -> {manifest}")


if __name__ == "__main__":
    main()
