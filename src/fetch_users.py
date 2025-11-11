# fetch_users.py
# Create and populate an SQLite DB from Polymarket JSONL + Goldsky subgraph.
# Tables: events, markets, assets, users, user_markets
# Logging: BASE_DIR/logs/users-YYYYMMDD-HHMMSS.log
#
# Env knobs:
#   PMDB_LOG_LEVEL=DEBUG | INFO | WARNING
#   PMDB_KEEP_FIRST_ASSETS=2  (use "all" to disable trimming)

import os
import json
import glob
import time
import logging
import sqlite3
from typing import Dict, Iterable, List, Optional
import argparse
from datetime import datetime, date, timedelta
import calendar

from utils import infer_winner, ensure_list, _market_start_date
from fetch_users_utils_par import get_all_takers

# -------------------------
# Paths and constants
# -------------------------
BASE_DIR    = os.path.expanduser("~/Documents/Projects/polymarket-db")
MARKETS_DIR = os.path.join(BASE_DIR, "markets")
DB_PATH     = os.path.join(BASE_DIR, "polymarket.db")
LOG_DIR     = os.path.join(BASE_DIR, "logs")
LOG_PATH    = os.path.join(LOG_DIR, f"users-{time.strftime('%Y%m%d-%H%M%S')}.log")

LOG_LEVEL = os.getenv("PMDB_LOG_LEVEL", "INFO").upper()

NUM_WORKERS = 5

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  slug TEXT,
  title TEXT,
  description TEXT
);

CREATE TABLE IF NOT EXISTS markets (
  condition_id TEXT PRIMARY KEY,
  slug TEXT,
  start_date TEXT,
  end_date TEXT,
  description TEXT,
  volume REAL,
  closed INTEGER,
  event_id TEXT,
  winner_asset_id TEXT,
  FOREIGN KEY(event_id) REFERENCES events(id)
);

CREATE INDEX IF NOT EXISTS idx_markets_event_id ON markets(event_id);

CREATE TABLE IF NOT EXISTS assets (
  asset_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  name TEXT,
  outcome_index INTEGER,
  FOREIGN KEY(market_id) REFERENCES markets(condition_id)
);

CREATE INDEX IF NOT EXISTS idx_assets_market_id ON assets(market_id);

CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS user_markets (
  user_id TEXT NOT NULL,
  market_id TEXT NOT NULL,
  PRIMARY KEY (user_id, market_id),
  FOREIGN KEY(user_id) REFERENCES users(id),
  FOREIGN KEY(market_id) REFERENCES markets(condition_id)
);
"""

# -------------------------
# Logger
# -------------------------
def get_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("polymarket_db")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logger.level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setLevel(logger.level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger

logger = get_logger()

# -------------------------
# DB helpers
# -------------------------
def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(BASE_DIR, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    logger.info(f"Initialized DB at {db_path}")
    return conn

def upsert_event(cur: sqlite3.Cursor, ev: Dict):
    cur.execute(
        """INSERT INTO events(id, slug, title, description)
           VALUES(?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             slug=excluded.slug,
             title=excluded.title,
             description=excluded.description""",
        (ev.get("id"), ev.get("slug"), ev.get("title"), ev.get("description")),
    )

def upsert_market(cur: sqlite3.Cursor, m: Dict, winner_asset_id: Optional[str], event_id: Optional[str]):
    cur.execute(
        """INSERT INTO markets(condition_id, slug, start_date, end_date, description, volume, closed, event_id, winner_asset_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(condition_id) DO UPDATE SET
             slug=excluded.slug,
             start_date=excluded.start_date,
             end_date=excluded.end_date,
             description=excluded.description,
             volume=excluded.volume,
             closed=excluded.closed,
             event_id=excluded.event_id,
             winner_asset_id=excluded.winner_asset_id""",
        (
            m.get("conditionId"),
            m.get("slug"),
            m.get("startDate"),
            m.get("endDate"),
            m.get("description"),
            float(m.get("volume") or 0.0),
            1 if m.get("closed") else 0,
            event_id,
            winner_asset_id,
        ),
    )

def upsert_asset(cur: sqlite3.Cursor, asset_id: str, market_id: str, name: Optional[str], idx: int):
    cur.execute(
        """INSERT INTO assets(asset_id, market_id, name, outcome_index)
           VALUES(?, ?, ?, ?)
           ON CONFLICT(asset_id) DO UPDATE SET
             market_id=excluded.market_id,
             name=excluded.name,
             outcome_index=excluded.outcome_index""",
        (asset_id, market_id, name, idx),
    )

def upsert_user_and_link(cur: sqlite3.Cursor, user_id: str, market_id: str):
    cur.execute("INSERT OR IGNORE INTO users(id) VALUES(?)", (user_id,))
    cur.execute("INSERT OR IGNORE INTO user_markets(user_id, market_id) VALUES(?, ?)", (user_id, market_id))

def count_users(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) FROM users")
    (n,) = cur.fetchone()
    return int(n)

# -------------------------
# JSONL iterator
# -------------------------
def iter_market_jsonl(files_glob: str) -> Iterable[Dict]:
    for path in sorted(glob.glob(files_glob)):
        logger.info(f"Reading file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                s = line.strip()
                if not s:
                    continue
                try:
                    yield json.loads(s)
                except json.JSONDecodeError as e:
                    logger.warning(f"Skip malformed JSON at {path}:{i}: {e}")

def _parse_ym(s: str) -> tuple[int, int]:
    # 'YYYY-MM' -> (year, month)
    dt = datetime.strptime(s, "%Y-%m")
    return dt.year, dt.month

def _parse_ymd(s: str) -> date:
    # 'YYYY-MM-DD' -> date
    return datetime.strptime(s, "%Y-%m-%d").date()

def list_jsonl_files_by_date(
    month: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
) -> List[str]:
    """Assumes filenames are MARKETS_DIR/YYYY-MM-DD.jsonl"""
    if not month and not from_date and not to_date:
        return sorted(glob.glob(os.path.join(MARKETS_DIR, "*.jsonl")))

    if month:
        y, m = _parse_ym(month)
        last_day = calendar.monthrange(y, m)[1]
        start_d = date(y, m, 1)
        end_d   = date(y, m, last_day)
    else:
        if not from_date and not to_date:
            return sorted(glob.glob(os.path.join(MARKETS_DIR, "*.jsonl")))
        if from_date and not to_date:
            start_d = end_d = _parse_ymd(from_date)
        elif to_date and not from_date:
            start_d = end_d = _parse_ymd(to_date)
        else:
            start_d = _parse_ymd(from_date)
            end_d   = _parse_ymd(to_date)
        if end_d < start_d:
            start_d, end_d = end_d, start_d

    files: List[str] = []
    d = start_d
    while d <= end_d:
        fp = os.path.join(MARKETS_DIR, d.strftime("%Y-%m-%d.jsonl"))
        if os.path.isfile(fp):
            files.append(fp)
        d += timedelta(days=1)

    if not files:
        logger.warning(f"No JSONL files found for range {start_d}..{end_d}")
    return files

# -------------------------
# Per-market processing
# -------------------------
def process_market(cur: sqlite3.Cursor, market: Dict):
    condition_id = market.get("conditionId")
    if not condition_id:
        return

    # events
    evs = market.get("events") or []
    if evs:
        for ev in evs:
            if ev.get("id"):
                upsert_event(cur, ev)
    event_id = market.get("events")[0].get("id")

    outcomes = [str(x) for x in ensure_list(market.get("outcomes"))]       # e.g. ["Yes","No"]
    asset_ids    = [str(x) for x in ensure_list(market.get("clobTokenIds"))]   # aligned asset ids
    prices   = ensure_list(market.get("outcomePrices"))                     # e.g. ["1","0"]

    slug = market.get("slug")
    start_date = _market_start_date(market)

    # infer winner
    winner_name, winner_id = infer_winner(outcomes, prices, asset_ids)
    outcome = (winner_name or "").strip().lower()

    upsert_market(cur, market, outcome, event_id)

    # upsert assets with names aligned to first N outcomes
    for idx, asset_id in enumerate(asset_ids):
        name = outcomes[idx] if idx < len(outcomes) else None
        upsert_asset(cur, asset_id, condition_id, name, idx)

    logger.info(f"[{start_date}] {slug}")

    # users via subgraph
    try:
        takers = get_all_takers(asset_ids)
    except Exception as e:
        logger.warning(f"Goldsky failure for market {condition_id}: {e}")
        takers = []
    for uid in takers:
        upsert_user_and_link(cur, uid, condition_id)

    logger.info(f"linked_users={len(takers)}")

import concurrent.futures, sqlite3
from itertools import islice

import sqlite3, time, random

def _process_market_parallel(m):
    for attempt in range(10):
        try:
            conn = sqlite3.connect(
                DB_PATH,
                timeout=30,
                check_same_thread=False,
                isolation_level=None,  # autocommit; we control txn
            )
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("PRAGMA busy_timeout=5000;")

                cur = conn.cursor()
                cur.execute("BEGIN;")
                process_market(cur, m)
                conn.commit()
                return
            finally:
                conn.close()
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "database is locked" in msg or "busy" in msg:
                #print("database locked")
                time.sleep(min(0.05 * (2 ** attempt), 2.0) + random.random() * 0.05)
                continue
            raise

def _batched(it, n):
    it = iter(it)
    while True:
        chunk = list(islice(it, n))
        if not chunk:
            return
        yield chunk

# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", help="YYYY-MM month to ingest, e.g. 2024-10")
    ap.add_argument("--from-date", dest="from_date", help="YYYY-MM-DD inclusive")
    ap.add_argument("--to-date",   dest="to_date",   help="YYYY-MM-DD inclusive")
    args = ap.parse_args()

    logger.info(f"Start. BASE_DIR={BASE_DIR}, MARKETS_DIR={MARKETS_DIR}, LOG_LEVEL={LOG_LEVEL}, "
                f"month={args.month}, from_date={args.from_date}, to_date={args.to_date}")
    
    if not os.path.isdir(MARKETS_DIR):
        logger.error(f"Missing markets dir: {MARKETS_DIR}")
        return

    t0 = time.time()
    conn = init_db(DB_PATH)
    cur  = conn.cursor()

    jsonl_files = list_jsonl_files_by_date(args.month, args.from_date, args.to_date)
    logger.info(f"Found {len(jsonl_files)} JSONL files")

    total_markets = 0
    total_users   = 0

    for fp in jsonl_files:
        users_before = count_users(cur)
        logger.info(f"Begin file transaction: {fp}")
        for batch in _batched(iter_market_jsonl(fp), 10):
            with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
                futs = [ex.submit(_process_market_parallel, m) for m in batch]
                for fut in concurrent.futures.as_completed(futs):
                    try:
                        fut.result()
                        total_markets += 1
                    except Exception as e:
                        logger.warning(f"Skip market due to error: {e}")

        users_after = count_users(cur)
        total_users = users_after
        logger.info(f"Committed file: {fp} | new_users={users_after - users_before} | total_users={total_users}")

    conn.commit()
    conn.close()

    dt = time.time() - t0
    logger.info(f"Done. markets_total={total_markets}, users_total={total_users}, elapsed_sec={dt:.1f}")
    logger.info(f"Log saved to: {LOG_PATH}")

if __name__ == "__main__":
    main()
