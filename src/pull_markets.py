#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"


def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Backfill Polymarket markets day-by-day into JSONL files. "
            "For each day, writes <outdir>/YYYY-MM-DD.jsonl "
            "with one raw market object per line."
        )
    )
    p.add_argument("--start", required=True, help="Start date (inclusive), e.g. 2024-10-01")
    p.add_argument("--end", required=True, help="End date (EXCLUSIVE), e.g. 2025-10-01")
    p.add_argument("--limit", type=int, default=500, help="Page size per request (default: 500)")
    p.add_argument("--min-volume", dest="min_volume", default="0",
                   help="volume_num_min filter (string or number, default: 0)")
    p.add_argument("--closed", default="true",
                   help='Pass "true" or "false" (string) to the "closed" param (default: "true")')
    p.add_argument("--outdir", default="markets", help='Base output folder (default: "markets")')
    p.add_argument("--timeout", type=int, default=60, help="HTTP timeout seconds (default: 60)")
    return p.parse_args()


# -------- rate limiting: ≤100 requests per 10 seconds --------
_req_times = deque()

def rate_limit(max_requests: int = 100, per_seconds: float = 10.0):
    now = time.time()
    while _req_times and (now - _req_times[0]) > per_seconds:
        _req_times.popleft()
    if len(_req_times) >= max_requests:
        sleep_for = per_seconds - (now - _req_times[0]) + 0.001
        if sleep_for > 0:
            time.sleep(sleep_for)
    _req_times.append(time.time())


def fetch_one_page(day_str: str, day_plus1_str: str, limit: int, offset: int,
                   min_volume: str, closed: str, timeout: int) -> List[Dict[str, Any]]:
    params = {
        "start_date_min": day_str,
        "start_date_max": day_plus1_str,
        "volume_num_min": str(min_volume),
        "closed": str(closed),
        "limit": str(limit),
    }
    if offset:
        params["offset"] = str(offset)

    rate_limit()
    for attempt in range(5):
        try:
            r = requests.get(GAMMA_URL, params=params, timeout=timeout)
            # Retry on throttle/5xx
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected payload type: {type(data)}")
            return data
        except Exception as e:
            wait = min(2 ** attempt, 10)
            if attempt < 4:
                print(f"[warn] {day_str} offset={offset} attempt {attempt+1}/5 failed: {e}. retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def fetch_all_for_day(day: datetime.date, limit: int, min_volume: str, closed: str, timeout: int) -> List[Dict[str, Any]]:
    day_str = day.strftime("%Y-%m-%d")
    day_plus1_str = (day + timedelta(days=1)).strftime("%Y-%m-%d")

    all_items: List[Dict[str, Any]] = []
    offset = 0
    page_idx = 0

    while True:
        items = fetch_one_page(day_str, day_plus1_str, limit, offset, min_volume, closed, timeout)
        n = len(items)
        if page_idx == 0 and n == limit:
            print(f"[limit] Exactly {limit} results on {day_str} → {day_plus1_str}")
        all_items.extend(items)
        if n < limit:
            break
        offset += limit
        page_idx += 1

    return all_items


def daterange(start_date: datetime.date, end_exclusive: datetime.date):
    cur = start_date
    while cur < end_exclusive:
        yield cur
        cur = cur + timedelta(days=1)


def main():
    args = parse_args()
    try:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end_exclusive = datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Invalid date: {e}", file=sys.stderr)
        sys.exit(1)

    out_base = Path(args.outdir)
    out_base.mkdir(parents=True, exist_ok=True)

    total_days = 0
    total_rows = 0

    for day in daterange(start, end_exclusive):
        total_days += 1
        day_str = day.strftime("%Y-%m-%d")
        try:
            items = fetch_all_for_day(day, args.limit, args.min_volume, args.closed, args.timeout)
        except Exception as e:
            print(f"[error] failed {day_str}: {e}")
            continue

        if (len(items) > 0):
            out_path = out_base / f"{day_str}.jsonl"

            with out_path.open("w", encoding="utf-8") as f:
                for obj in items:
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")

            total_rows += len(items)
            print(f"[{day_str}] wrote {len(items)} markets → {out_path}")
        else: 
            print(f"[{day_str}] has 0 markets")
        

    print(f"Done. Days processed: {total_days}. Total markets written: {total_rows}. Base dir: {out_base.resolve()}")


if __name__ == "__main__":
    main()
