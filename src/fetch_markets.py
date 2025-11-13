# fetch_markets.py
# Parallel, rate-limited Polymarket markets backfill.
# - Writes markets/YYYY-MM-DD.jsonl
# - Logs to logs/markets-YYYYMMDD-HHMMSS.log
# - Fixed paging at offsets 0, 500, 1000 (limit=500)
# - If all three pages are full (1500 rows), logs ERROR and moves on
# - --resume skips days with an existing non-empty file
# - Concurrency, rate limit, and log level via env:
#     PMDB_CONCURRENCY=8
#     PMDB_RATE_MAX=100
#     PMDB_RATE_WINDOW=10
#     PMDB_LOG_LEVEL=INFO

import argparse
import json
import logging
import os
import sys
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
OUTPUT_DIR = Path("markets")
LOG_DIR = Path("logs")

TIMEOUT = 60

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Backfill Polymarket markets day-by-day into JSONL files. "
            "Writes markets/YYY-MM-DD.jsonl. Parallel across days with a global 100 req / 10 s cap."
        )
    )
    p.add_argument("--start", default="2021-01-01", help='Start date (inclusive), Defaults to "2021-01-01" (start of Polymarket)')
    p.add_argument("--end", default=datetime.now().date().strftime("%Y-%m-%d"),
                   help='End date (exclusive). Defaults to today.')
    p.add_argument("--resume", action="store_true", help="Skip days with an existing .jsonl file")
    return p.parse_args()

# ---------- Logging ----------
def setup_logging() -> str:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = LOG_DIR / f"markets-{ts}.log"
    lvl = os.getenv("PMDB_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stderr),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    return str(log_path)

# ---------- Rate limiter ----------
_RATE_LOCK = Lock()
_REQ_TIMES: deque[float] = deque()
MAX_REQ = int(os.getenv("PMDB_RATE_MAX", "100"))
WINDOW_S = float(os.getenv("PMDB_RATE_WINDOW", "10"))

def rate_limit():
    now = time.time()
    with _RATE_LOCK:
        while _REQ_TIMES and (now - _REQ_TIMES[0]) > WINDOW_S:
            _REQ_TIMES.popleft()
        if len(_REQ_TIMES) >= MAX_REQ:
            sleep_for = WINDOW_S - (now - _REQ_TIMES[0]) + 0.001
            if sleep_for > 0:
                time.sleep(sleep_for)
            now2 = time.time()
            while _REQ_TIMES and (now2 - _REQ_TIMES[0]) > WINDOW_S:
                _REQ_TIMES.popleft()
        _REQ_TIMES.append(time.time())

# ---------- Helpers ----------
def daterange(start_date: date, end_exclusive: date):
    cur = start_date
    while cur < end_exclusive:
        yield cur
        cur += timedelta(days=1)

def out_path_for_day(d: date) -> Path:
    return OUTPUT_DIR / f"{d.strftime('%Y-%m-%d')}.jsonl"

def _iso(dt: datetime) -> str:
    # format as RFC3339 UTC, e.g. 2025-09-15T00:00:00Z
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- HTTP fetch ----------

def fetch_window(
    start_dt: datetime,
    end_dt: datetime,
) -> tuple[list[dict[str, Any]], bool]:
    """
    Fetch a [start_dt, end_dt) window using fixed offsets 0/500/1000.
    Returns (items, capped_flag).
    """
    LIMIT = 500
    items: List[Dict[str, Any]] = []
    capped = False
    start_iso = _iso(start_dt)
    end_iso = _iso(end_dt)

    with requests.Session() as session:
        page0 = fetch_one_page(session, start_iso, end_iso, LIMIT, 0)
        items.extend(page0)
        if len(page0) == LIMIT:
            page1 = fetch_one_page(session, start_iso, end_iso, LIMIT, 500)
            items.extend(page1)
            if len(page1) == LIMIT:
                page2 = fetch_one_page(session, start_iso, end_iso, LIMIT, 1000)
                items.extend(page2)
                if len(page2) == LIMIT:
                    capped = True
    return items, capped

def fetch_one_page(
    session: requests.Session,
    start_iso: str,
    end_iso: str,
    limit: int,
    offset: int,
    max_retries: int = 5,
) -> List[Dict[str, Any]]:
    params = {
        "start_date_min": start_iso,
        "start_date_max": end_iso,
        "limit": str(limit),         # fixed 500
    }
    if offset:
        params["offset"] = str(offset)

    for attempt in range(1, max_retries + 1):
        rate_limit()
        try:
            r = session.get(GAMMA_URL, params=params, timeout=TIMEOUT)
            sc = r.status_code
            if sc == 429 or sc >= 500:
                raise requests.HTTPError(f"HTTP {sc}")
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected payload type: {type(data)}")
            if attempt > 1:
                logging.debug("Recovered: start=%s end=%s offset=%d attempt=%d", start_iso, end_iso, offset, attempt)
            return data
        except Exception as e:
            backoff = min(2 ** (attempt - 1), 10)
            if attempt < max_retries:
                logging.warning("Fetch fail start=%s end=%s offset=%d attempt=%d/%d: %s. Retry in %ss",
                                start_iso, end_iso, offset, attempt, max_retries, e, backoff)
                time.sleep(backoff)
            else:
                logging.error("Permanent fail start=%s end=%s offset=%d: %s", start_iso, end_iso, offset, e)
                raise

# ---------- Per-day pipeline ----------
def fetch_day_to_file(
    d: date,
    resume: bool,
) -> Tuple[date, int, Optional[str]]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = out_path_for_day(d)
    if resume and path.exists() and path.stat().st_size > 0:
        logging.info("Resume skip %s", path.name)
        return (d, 0, None)

    day_start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    day_end   = day_start + timedelta(days=1)
    mid       = day_start + timedelta(hours=12)

    # First try full day
    items, capped = fetch_window(day_start, day_end)

    capped_err: Optional[str] = None
    if not capped:
        # Write and return
        with path.open("w", encoding="utf-8") as f:
            for obj in items:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        logging.info("[%s] wrote %d markets -> %s", d.strftime("%Y-%m-%d"), len(items), path)
        return (d, len(items), None)

    # Split into two half-days if capped
        # Split into two half-days if capped
    logging.warning("Day %s capped at 1500. Splitting into two half-day windows.", d.strftime("%Y-%m-%d"))

    items_am, capped_am = fetch_window(day_start, mid)
    items_pm, capped_pm = fetch_window(mid, day_end)

    # If both halves capped, split into quarters [0-6), [6-12), [12-18), [18-24)
    if capped_am and capped_pm:
        logging.warning("Both halves capped on %s. Splitting into four 6-hour quarters.", d.strftime("%Y-%m-%d"))
        q1_start, q1_end = day_start, day_start + timedelta(hours=6)
        q2_start, q2_end = q1_end,     day_start + timedelta(hours=12)
        q3_start, q3_end = q2_end,     day_start + timedelta(hours=18)
        q4_start, q4_end = q3_end,     day_end

        q1_items, q1_cap = fetch_window(q1_start, q1_end)
        q2_items, q2_cap = fetch_window(q2_start, q2_end)
        q3_items, q3_cap = fetch_window(q3_start, q3_end)
        q4_items, q4_cap = fetch_window(q4_start, q4_end)

        if q1_cap or q2_cap or q3_cap or q4_cap:
            msg = (
                f"Day {d.strftime('%Y-%m-%d')} still capped after quarter split. "
                "Reduce window below 6 hours."
            )
            logging.error(msg)
            raise RuntimeError(msg)

        items = q1_items + q2_items + q3_items + q4_items
        total = len(items)

        with path.open("w", encoding="utf-8") as f:
            for obj in items:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")

        logging.info("[%s] wrote %d markets -> %s (quarter split)", d.strftime("%Y-%m-%d"), total, path)
        return (d, total, None)

    # Else: one or zero halves capped → write what we have, warn if truncated
    total = len(items_am) + len(items_pm)
    with path.open("w", encoding="utf-8") as f:
        for obj in items_am:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        for obj in items_pm:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    if capped_am or capped_pm:
        capped_err = f"Day {d.strftime('%Y-%m-%d')} had one half capped; wrote {total} but may be truncated."
        logging.error(capped_err)
    else:
        capped_err = None

    logging.info("[%s] wrote %d markets -> %s (half split)", d.strftime("%Y-%m-%d"), total, path)
    return (d, total, capped_err)



# ---------- Main ----------
def main():
    args = parse_args()

    # Dates
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_exclusive = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Invalid date: {e}", file=sys.stderr)
        sys.exit(1)
    if end_exclusive <= start:
        print("End must be after start", file=sys.stderr)
        sys.exit(1)

    log_path = setup_logging()
    logging.info("Start %s → %s, out=markets/, resume=%s", start, end_exclusive, args.resume)

    days = list(daterange(start, end_exclusive))
    if not days:
        logging.warning("No days in range")
        return

    total_rows = 0
    total_days = 0
    capped_days = 0

    workers = int(os.getenv("PMDB_CONCURRENCY", "8"))
    futures = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for d in days:
            # quick pre-skip
            if args.resume:
                p = out_path_for_day(d)
                if p.exists() and p.stat().st_size > 0:
                    logging.info("Resume pre-skip %s", p.name)
                    continue

            futures.append(ex.submit(fetch_day_to_file, d, args.resume))

        for fut in as_completed(futures):
            try:
                d, n, err = fut.result()
                total_days += 1
                total_rows += n
                if err:
                    capped_days += 1
            except Exception as e:
                logging.error("Worker crashed: %s", e)

    logging.info("Done. Days processed: %d. Total markets written: %d. Capped days: %d. Log: %s",
                 total_days, total_rows, capped_days, log_path)

if __name__ == "__main__":
    main()
