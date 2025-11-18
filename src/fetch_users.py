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
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional
import argparse
from datetime import datetime, date, timedelta
import calendar
from itertools import islice
import random

from utils import infer_winner, ensure_list, _market_start_date
from fetch_users_utils import get_all_takers

# -------------------------
# Paths and constants
# -------------------------
BASE_DIR    = os.path.expanduser("D:\Projects\polymarket-db")
MARKETS_DIR = os.path.join(BASE_DIR, "new_markets")
DB_PATH     = "C:/Users/nimro/polymarket-db-C/polymarket.db"
LOG_DIR     = os.path.join(BASE_DIR, "logs")
LOG_PATH    = os.path.join(LOG_DIR, f"users-{time.strftime('%Y%m%d-%H%M%S')}.log")

LOG_LEVEL = os.getenv("PMDB_LOG_LEVEL", "INFO").upper()

NUM_WORKERS = 20

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

def load_known_markets(cur: sqlite3.Cursor) -> set[str]:
    cur.execute("SELECT condition_id FROM markets WHERE volume IS NOT NULL")
    rows = cur.fetchall()
    known = {r[0] for r in rows if r[0]}
    logger.info(f"Loaded {len(known)} existing markets from DB for skipping")
    return known

# -------------------------
# JSONL iterator
# -------------------------
def iter_market_jsonl(files_glob: str) -> Iterable[Dict]:
    for path in sorted(glob.glob(files_glob)):
        #logger.info(f"Reading file: {path}")
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
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[str]:
    """Assumes filenames are MARKETS_DIR/YYYY-MM-DD.jsonl"""
    if not month and not start and not end:
        return sorted(glob.glob(os.path.join(MARKETS_DIR, "*.jsonl")))

    if month:
        y, m = _parse_ym(month)
        last_day = calendar.monthrange(y, m)[1]
        start_d = date(y, m, 1)
        end_d   = date(y, m, last_day)
    else:
        if not start and not end:
            return sorted(glob.glob(os.path.join(MARKETS_DIR, "*.jsonl")))
        if start and not end:
            start_d = end_d = _parse_ymd(start)
        elif end and not start:
            start_d = end_d = _parse_ymd(end)
        else:
            start_d = _parse_ymd(start)
            end_d   = _parse_ymd(end)
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

def _extract_asset_ids(market: dict) -> list[str]:
    return [str(x) for x in ensure_list(market.get("clobTokenIds"))]

def _fetch_users_for_market(market: dict) -> list[str]:
    asset_ids = _extract_asset_ids(market)
    try:
        return sorted(set(get_all_takers(asset_ids)))
    except Exception as e:
        logger.warning(f"Goldsky failure for market {market.get('conditionId')}: {e}")
        return []

def _write_market_jsonl(market: dict) -> str:
    """Create one-line JSONL file: {'market': <market_dict>, 'users': [..]}."""
    data = {"market": market, "users": _fetch_users_for_market(market)}
    fd, path = tempfile.mkstemp(prefix="market_", suffix=".jsonl")
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    return path

def upsert_market_bundle(cur: sqlite3.Cursor, market: dict, users: list[str]) -> None:
    condition_id = market.get("conditionId")
    if not condition_id:
        return

    evs = market.get("events") or []
    for ev in evs:
        if ev.get("id"):
            upsert_event(cur, ev)
    event_id = (market.get("events") or [{}])[0].get("id")

    outcomes   = [str(x) for x in ensure_list(market.get("outcomes"))]
    asset_ids  = [str(x) for x in ensure_list(market.get("clobTokenIds"))]
    prices     = ensure_list(market.get("outcomePrices"))

    winner_name, winner_id = infer_winner(outcomes, prices, asset_ids)
    outcome = (winner_name or "").strip().lower()

    upsert_market(cur, market, outcome, event_id)

    for idx, asset_id in enumerate(asset_ids):
        name = outcomes[idx] if idx < len(outcomes) else None
        upsert_asset(cur, asset_id, condition_id, name, idx)

    for uid in users:
        upsert_user_and_link(cur, uid, condition_id)

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
    ap.add_argument("--start", dest="start", help="YYYY-MM-DD inclusive")
    ap.add_argument("--end",   dest="end",   help="YYYY-MM-DD inclusive")
    ap.add_argument("--resume", action="store_true", help="Skip markets that were already upserted to db")
    args = ap.parse_args()

    logger.info(f"Start. BASE_DIR={BASE_DIR}, MARKETS_DIR={MARKETS_DIR}, LOG_LEVEL={LOG_LEVEL}, "
                f"month={args.month}, start={args.start}, end={args.end}")
    
    if not os.path.isdir(MARKETS_DIR):
        logger.error(f"Missing markets dir: {MARKETS_DIR}")
        return

    t0 = time.time()
    conn = init_db(DB_PATH)
    cur  = conn.cursor()

    known_markets = load_known_markets(cur)

    jsonl_files = list_jsonl_files_by_date(args.month, args.start, args.end)
    logger.info(f"Found {len(jsonl_files)} JSONL files")

    total_markets = 0
    total_users   = 0

    for fp in jsonl_files:
        users_before = count_users(cur)
        #logger.info(f"Begin file transaction: {fp}")
        for batch in _batched(
            (
                m
                for m in iter_market_jsonl(fp)
                if not (args.resume and m.get("conditionId") and m["conditionId"] in known_markets)
            ),
            10,
        ):

            # Phase 1: parallel fetch → per-market JSONL paths
            paths = []
            with ThreadPoolExecutor(max_workers=NUM_WORKERS) as ex:
                futs = [ex.submit(_write_market_jsonl, m) for m in batch]
                for fut in as_completed(futs):
                    try:
                        paths.append(fut.result())
                    except Exception as e:
                        logger.warning(f"Fetch skipped due to error: {e}")

            # Phase 2: single-writer upsert
            cur.execute("BEGIN;")
            try:
                for p in paths:
                    with open(p, "r") as f:
                        rec = json.loads(f.readline())
                    upsert_market_bundle(cur, rec["market"], rec["users"])
                    total_markets += 1
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.warning(f"Batch upsert rolled back: {e}")
            finally:
                for p in paths:
                    try:
                        os.remove(p)
                    except OSError:
                        pass

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
