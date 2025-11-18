#!/usr/bin/env python
"""
ingest_raw_trades.py

Bulk-ingest per-user raw trades JSONL files into a SQLite DB.

- Raw trades: ./raw_trades/<shard1>/<shard2>/<wallet>.jsonl
- Database:   ./polymarket.db
- Optimizations:
  * Iterate by users from DB (no expensive rglob if not needed)
  * Single DB connection
  * Large transactions (commit every N wallets)
  * Resume mode using in-memory set of already-ingested wallets
"""

import os
import json
import sqlite3
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Set
from datetime import datetime, timezone
import signal

STOP = False

def _sigint_handler(signum, frame):
    global STOP
    STOP = True
    print("\n[!] SIGINT received, stopping after current iteration…")

signal.signal(signal.SIGINT, _sigint_handler)

# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--raw-dir",
        default="./raw_trades",
        help="Root folder with sharded per-user *.jsonl (default ./raw_trades)",
    )
    ap.add_argument(
        "--db-path",
        default="C:/Users/nimro/polymarket-db-C/polymarket.db",
        help="SQLite DB path (default ./polymarket.db)",
    )
    ap.add_argument(
        "--resume",
        action="store_true",
        help="Skip wallets that already have trades in DB (based on trades.proxy_wallet)",
    )
    ap.add_argument(
        "--log-level",
        default=os.getenv("PMDB_LOG_LEVEL", "INFO").upper(),
        help="Log level, e.g. DEBUG/INFO/WARNING (default INFO or PMDB_LOG_LEVEL)",
    )
    ap.add_argument(
        "--log-dir",
        default="./logs",
        help="Directory for log files (default ./logs)",
    )
    ap.add_argument(
        "--batch-commit",
        type=int,
        default=1000,
        help="Commit after this many wallets (default 1000)",
    )
    ap.add_argument(
        "--max-users",
        type=int,
        default=0,
        help="Optional cap on number of users to process (0 = all)",
    )
    return ap.parse_args()


# ---------- Logging ----------

def setup_logger(log_dir: Path, level: str) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"ingest-raw-{ts}.log"

    logger = logging.getLogger("ingest_raw_trades")
    logger.setLevel(getattr(logging, level, logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logger.level)

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logger.level)

    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info(f"Log file: {log_path}")
    return logger


# ---------- Small utils ----------

def utc_iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None

def pick_first_nonempty(*vals):
    for v in vals:
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def get_nested(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def sha1_hex(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def decimal_fmt(x) -> str:
    try:
        return f"{float(x):.10f}"
    except Exception:
        return "0.0000000000"


# ---------- Sharding helper (must match your fetcher) ----------

def _shard_parts(wallet: str) -> Tuple[str, str]:
    """
    Simple sharding: use first 2 and next 2 hex chars after '0x'.
    Adjust if your fetcher uses a different scheme.
    """
    w = wallet.lower()
    if w.startswith("0x"):
        w = w[2:]
    a = w[:2] or "xx"
    b = w[2:4] or "yy"
    return a, b

def raw_path_for_user(raw_trades_dir: Path, wallet: str) -> Path:
    a, b = _shard_parts(wallet)
    return raw_trades_dir / a / b / f"{wallet}.jsonl"


# ---------- Trade key + field extraction ----------

def extract_ts(row: Dict[str, Any]) -> Optional[int]:
    for k in ("timestamp", "time", "createdAt"):
        v = row.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    v = get_nested(row, "payload", "timestamp")
    return v if isinstance(v, int) else None

def extract_field(row: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    if "payload" in row and isinstance(row["payload"], dict):
        for k in keys:
            if k in row["payload"] and row["payload"][k] not in (None, ""):
                return row["payload"][k]
    return default

def compute_trade_uid(row: Dict[str, Any]) -> str:
    proxy = str(extract_field(row, ["proxyWallet", "wallet", "user"]))
    ts = str(extract_ts(row) or "")
    cond = str(extract_field(row, ["conditionId", "condition_id", "market"]))
    asset = str(extract_field(row, ["asset", "clobTokenId", "tokenId"]))
    side = str(extract_field(row, ["side"]))
    price = decimal_fmt(extract_field(row, ["price"]))
    size = decimal_fmt(extract_field(row, ["size", "amount"]))
    txh = str(extract_field(row, ["transactionHash", "txHash", "tx"]))
    return sha1_hex("|".join([proxy, ts, cond, asset, side, price, size, txh]))

def row_to_trade_tuple(row: Dict[str, Any]) -> Tuple:
    proxy_wallet = str(extract_field(row, ["proxyWallet", "wallet", "user"]) or "")
    timestamp = int(extract_ts(row) or 0)
    condition_id = str(extract_field(row, ["conditionId", "condition_id", "market"]) or "")
    size = float(extract_field(row, ["size", "amount"]) or 0.0)
    usdc_size = float(extract_field(row, ["usdcSize", "quote"]) or 0.0)
    transaction_hash = str(extract_field(row, ["transactionHash", "txHash", "tx"]) or "")
    price = float(extract_field(row, ["price"]) or 0.0)
    asset = str(extract_field(row, ["asset", "clobTokenId", "tokenId"]) or "")
    side = str(extract_field(row, ["side"]) or "")
    outcome_index = extract_field(row, ["outcomeIndex", "outcome_index"])
    try:
        outcome_index = int(outcome_index) if outcome_index is not None else None
    except Exception:
        outcome_index = None
    trade_uid = compute_trade_uid(row)
    return (
        trade_uid,
        proxy_wallet,
        timestamp,
        condition_id,
        size,
        usdc_size,
        transaction_hash,
        price,
        asset,
        side,
        outcome_index,
    )

def extract_market_meta_from_row(row: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    cond = str(extract_field(row, ["conditionId", "condition_id", "market"]) or "")
    slug = extract_field(row, ["slug"])
    desc = pick_first_nonempty(
        extract_field(row, ["title"]),
        extract_field(row, ["name"]),
    )
    return cond, slug, desc

def extract_user_meta_from_rows(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    name = None
    pseudonym = None
    for r in rows:
        name = name or pick_first_nonempty(
            str(extract_field(r, ["name"]) or ""),
            str(get_nested(r, "user", "name") or ""),
            str(get_nested(r, "user", "username") or ""),
        )
        pseudonym = pseudonym or pick_first_nonempty(
            str(extract_field(r, ["pseudonym"]) or ""),
            str(get_nested(r, "user", "pseudonym") or ""),
        )
        if name and pseudonym:
            break
    return name, pseudonym


# ---------- DB helpers ----------

def db_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
    # EXTREME bulk settings – use only on a disposable copy
    conn.execute("PRAGMA journal_mode=OFF;")        # no WAL, no rollback journal
    conn.execute("PRAGMA synchronous=OFF;")         # never fsync
    conn.execute("PRAGMA locking_mode=EXCLUSIVE;")  # grab exclusive lock; no other readers
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-400000;")      # ~400 MB page cache (adjust if needed)
    conn.execute("PRAGMA foreign_keys=OFF;")
    return conn


def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    # trades
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades(
      trade_uid TEXT PRIMARY KEY,
      proxy_wallet TEXT NOT NULL,
      timestamp INTEGER NOT NULL,
      condition_id TEXT,
      size REAL, usdc_size REAL,
      transaction_hash TEXT, price REAL,
      asset TEXT, side TEXT, outcome_index INTEGER
    );""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_user_time ON trades(proxy_wallet, timestamp);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(condition_id);")

    # markets (minimal placeholders)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS markets(
      condition_id TEXT PRIMARY KEY,
      slug TEXT,
      start_date TEXT,
      description TEXT
    );""")

    # users (minimal)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
      id TEXT PRIMARY KEY,
      name TEXT,
      pseudonym TEXT
    );""")

    # add columns if missing (idempotent)
    def col_exists(table: str, col: str) -> bool:
        return any(c[1] == col for c in conn.execute(f"PRAGMA table_info({table});").fetchall())

    if not col_exists("users", "name"):
        cur.execute("ALTER TABLE users ADD COLUMN name TEXT;")
    if not col_exists("users", "pseudonym"):
        cur.execute("ALTER TABLE users ADD COLUMN pseudonym TEXT;")

    cur.close()

def get_all_user_ids(conn: sqlite3.Connection) -> List[str]:
    try:
        rows = conn.execute("SELECT id FROM users;").fetchall()
        return [str(r[0]) for r in rows if r and r[0]]
    except Exception:
        return []

def get_ingested_wallets(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute("SELECT DISTINCT proxy_wallet FROM trades;").fetchall()
    return {str(r[0]).lower() for r in rows if r and r[0]}


def upsert_trades(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> Tuple[int, Set[str]]:
    if not rows:
        return 0, set()
    cur = conn.cursor()
    inserted = 0
    markets_seen: Set[str] = set()
    for r in rows:
        tpl = row_to_trade_tuple(r)
        condition_id = tpl[3]
        markets_seen.add(condition_id)
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO trades(
                    trade_uid, proxy_wallet, timestamp, condition_id, size, usdc_size,
                    transaction_hash, price, asset, side, outcome_index
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?);
                """,
                tpl,
            )
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            # ignore single-row issues; this is bulk ingest
            pass
    cur.close()
    return inserted, markets_seen


def ensure_market_placeholders(
    conn: sqlite3.Connection,
    wallet: str,
    cond_to_meta: Dict[str, Tuple[Optional[str], Optional[str], Optional[int]]],
    logger: logging.Logger,
) -> int:
    if not cond_to_meta:
        return 0
    cur = conn.cursor()
    new_count = 0
    for cond, (slug, desc, oldest_ts) in cond_to_meta.items():
        if not cond:
            continue
        row = cur.execute(
            "SELECT 1 FROM markets WHERE condition_id=?;",
            (cond,),
        ).fetchone()
        if row:
            continue
        start_date_iso = utc_iso(oldest_ts) if oldest_ts else None
        cur.execute(
            "INSERT OR IGNORE INTO markets(condition_id, slug, start_date, description) VALUES (?,?,?,?);",
            (cond, slug, start_date_iso, desc),
        )
        if cur.rowcount > 0:
            new_count += 1
            logger.warning(f"[new-market] user={wallet} market={cond} upserted placeholder")
    cur.close()
    return new_count


def update_user_metadata(
    conn: sqlite3.Connection,
    wallet: str,
    name: Optional[str],
    pseudonym: Optional[str],
) -> None:
    if not wallet:
        return
    conn.execute("INSERT OR IGNORE INTO users(id) VALUES (?);", (wallet,))
    row = conn.execute(
        "SELECT name, pseudonym FROM users WHERE id=?;",
        (wallet,),
    ).fetchone()
    cur_name, cur_pseudo = row if row else (None, None)

    set_name = bool(name and not (cur_name and str(cur_name).strip()))
    set_pseudo = bool(pseudonym and not (cur_pseudo and str(cur_pseudo).strip()))

    if set_name or set_pseudo:
        conn.execute(
            "UPDATE users SET name=COALESCE(?,name), pseudonym=COALESCE(?,pseudonym) WHERE id=?;",
            (name if set_name else None, pseudonym if set_pseudo else None, wallet),
        )


# ---------- I/O ----------

def read_jsonl(fp: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with fp.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except json.JSONDecodeError:
                continue
    return out

def build_market_meta(rows: List[Dict[str, Any]]) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[int]]]:
    meta: Dict[str, Tuple[Optional[str], Optional[str], Optional[int]]] = {}
    for r in rows:
        cond, slug, desc = extract_market_meta_from_row(r)
        ts = extract_ts(r)
        if cond not in meta:
            meta[cond] = (slug, desc, ts)
        else:
            oslug, odesc, ots = meta[cond]
            meta[cond] = (
                oslug or slug,
                odesc or desc,
                min(ots, ts) if (ots and ts) else (ots or ts),
            )
    return meta


# ---------- Main ----------

def main() -> None:
    args = parse_args()
    logger = setup_logger(Path(args.log_dir), args.log_level)

    raw_dir = Path(args.raw_dir)
    db_path = Path(args.db_path)

    if not raw_dir.exists():
        logger.error(f"Missing raw dir: {raw_dir}")
        return

    conn = db_connect(db_path)
    ensure_tables(conn)
    print("getting users")
    users = get_all_user_ids(conn)
    users = [u.lower() for u in users if u]
    print("sorting users")
    users.sort()
    users = users[480000::]
    if args.max_users > 0:
        users = users[: args.max_users]

    if args.resume:
        print("getting ingested wallets")
        ingested = get_ingested_wallets(conn)
        logger.info(f"[resume] already-ingested wallets={len(ingested)}")
    else:
        ingested = set()

    logger.info(
        f"Start ingest | raw_dir={raw_dir} db={db_path} "
        f"users={len(users)} resume={args.resume} batch_commit={args.batch_commit}"
    )

    total_users = 0
    total_skipped = 0
    total_rows = 0
    total_inserted = 0
    total_new_markets = 0
    failed = 0
    cur_inserted = 0
    cur_rows = 0

    conn.execute("BEGIN;")
    batch_counter = 0

    for wallet in users:
        
        if STOP:
            logger.warning("Gracefully stopping due to Ctrl-C")
            break
        
        

        total_users += 1

        fp = raw_path_for_user(raw_dir, wallet)

        if args.resume and wallet in ingested:
            total_skipped += 1
            continue

        if not fp.is_file():
            #logger.warning(f"[no-file] wallet={wallet}")
            failed += 1
            continue

        try:
            rows = read_jsonl(fp)
        except Exception as e:
            logger.warning(f"[skip-file] wallet={wallet} path={fp} read error: {e}")
            failed += 1
            continue

        if not rows:
            logger.info(f"[skip-empty] wallet={wallet} path={fp}")
            failed += 1
            continue

        # user meta
        name, pseudo = extract_user_meta_from_rows(rows)
        update_user_metadata(conn, wallet, name, pseudo)

        # market placeholders
        cond_meta = build_market_meta(rows)
        new_mk = ensure_market_placeholders(conn, wallet, cond_meta, logger)
        total_new_markets += new_mk

        # trades
        inserted, _ = upsert_trades(conn, rows)
        total_rows += len(rows)
        total_inserted += inserted
        cur_inserted += inserted
        cur_rows += len(rows)

        if (total_users % 1000 == 0):
            logger.info(f"total_users={total_users}, tot_rows={total_rows}, tot_insert={total_inserted}, total_skipped={total_skipped}, total_failed={failed}")
            cur_inserted = cur_inserted = 0
        
        #logger.info(
        #    f"[ingest] wallet={wallet} file_rows={len(rows)} "
        #    f"inserted={inserted} new_markets={new_mk}"
        #)

        if args.resume and inserted > 0:
            ingested.add(wallet)

        batch_counter += 1
        if batch_counter >= args.batch_commit:
            conn.commit()
            conn.execute("BEGIN;")
            batch_counter = 0

    conn.commit()
    conn.close()

    logger.info(
        f"Done | users_processed={total_users} users_skipped={total_skipped} "
        f"rows_total={total_rows} inserted_new={total_inserted} new_markets={total_new_markets}"
    )


if __name__ == "__main__":
    main()
