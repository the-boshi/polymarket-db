#!/usr/bin/env python3
# fetch_trades.py
# Async fetch Polymarket user trades, save per-user JSONL, and ingest into SQLite via a single-writer queue.
# Requirements fulfilled per spec: global sliding-window rate limit, retries with backoff and page-size fallback,
# offset-first fetch + time-window fallback, per-user JSONL, single-writer queue, schema ensure, placeholders.

import os
import sys
import json
import time
import math
import argparse
import asyncio
import logging
import sqlite3
import random
import hashlib
from decimal import Decimal, InvalidOperation, ROUND_DOWN, getcontext
from pathlib import Path
from datetime import datetime, timezone
from collections import deque, defaultdict
from typing import Any, Dict, List, Tuple, Optional

import aiohttp

API_URL = "https://data-api.polymarket.com/activity"
BASE_DIR = "D:/Projects/polymarket-db"

# default DEBUG on Windows, else INFO (overridable via PMDB_LOG_LEVEL)
IS_WINDOWS = (os.name == "nt") or (platform.system() == "Windows")
LOG_LEVEL = os.getenv("PMDB_LOG_LEVEL", "DEBUG" if IS_WINDOWS else "INFO").upper()

class HttpRetryExhausted(Exception):
    pass

# ---------- Utilities ----------

def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default

def env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return v if v and v.strip() else default

def now_utc_ts() -> int:
    return int(time.time())

def ts_to_batch(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d-%H%M%S")

def utc_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")

def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def decimal_fmt(x: Any) -> str:
    # Format as fixed 10 decimals, no scientific, round down.
    getcontext().prec = 40
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        d = Decimal(0)
    q = d.quantize(Decimal("0.0000000001"), rounding=ROUND_DOWN)
    # normalize to exactly 10 decimals
    s = f"{q:.10f}"
    return s

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=False)

def pick_first_nonempty(*vals) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None

def get_nested(d: Dict[str, Any], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _shard_parts(wallet: str) -> tuple[str, str]:
    w = wallet.lower().lstrip("0x")
    if len(w) < 4:
        w = w.ljust(4, "0")
    return w[:2], w[2:4]

# ---------- Logger ----------

def setup_logger(base_dir: Path, batch_tag: str, level_name: str) -> logging.Logger:
    logs_dir = base_dir / "logs"
    ensure_dir(logs_dir)
    log_path = logs_dir / f"trades-{batch_tag}.log"



    logger = logging.getLogger("pmdb")
    #logger.setLevel(getattr(logging, level_name.upper(), logging.INFO))
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    fh = logging.FileHandler(log_path, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh.setFormatter(fmt)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger

# ---------- Rate Limiter (sliding window) ----------

class SlidingWindowLimiter:
    def __init__(self, max_calls: int, window_sec: float, logger: logging.Logger):
        self.max_calls = max_calls
        self.window = window_sec
        self.logger = logger
        self._dq = deque()  # monotonic times
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                # drop stale
                while self._dq and (now - self._dq[0]) > self.window:
                    self._dq.popleft()
                if len(self._dq) < self.max_calls:
                    self._dq.append(now)
                    return
                # need to wait
                wait = self.window - (now - self._dq[0]) + random.uniform(0.01, 0.05)
                wait = max(0.01, min(wait, self.window))
                #self.logger.debug(f"[rate] stall {wait:.3f}s; q={len(self._dq)}/{self.max_calls}")
            await asyncio.sleep(wait)

# ---------- HTTP GET with retries ----------

RETRY_STATUSES = {408, 409, 420, 429, 500, 502, 503, 504}

async def api_get_json(
    session: aiohttp.ClientSession,
    limiter: SlidingWindowLimiter,
    params: Dict[str, Any],
    logger: logging.Logger,
    max_retries: int = 7,
) -> Optional[List[Dict[str, Any]]]:
    """
    Returns a list of activity rows or None on non-retriable 4xx.
    On 503 after a retry, page size fallback: 500→250→100→50.
    """
    # apply page-size fallback sequence if 'limit' present
    limit_seq = [params.get("limit", 500)]
    limit_seq = [int(limit_seq[0]) if limit_seq[0] is not None else 500]
    # normalize allowed sizes
    allowed = [500, 250, 100, 50]
    if limit_seq[0] not in allowed:
        limit_seq = [500]
    for s in allowed:
        if s != limit_seq[0]:
            limit_seq.append(s)

    backoff = 0.5
    attempt = 0
    lim_idx = 0

    while attempt <= max_retries:
        attempt += 1
        await limiter.acquire()
        try:
            async with session.get(API_URL, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                status = resp.status
                text = await resp.text()

                if 200 <= status < 300:
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        logger.warning(f"[http] JSON parse error len={len(text)}; params={params}")
                        data = None
                    # The endpoint returns a list; if dict, try common keys.
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        for key in ("data", "items", "result", "activity"):
                            if isinstance(data.get(key), list):
                                return data[key]
                        # fallback: if dict contains 'rows' as list
                        if isinstance(data.get("rows"), list):
                            return data["rows"]
                        return []
                    return []
                # Non-2xx
                if status in RETRY_STATUSES:
                    # 503 fallback after first retry
                    if status == 503 and attempt >= 2 and "limit" in params:
                        if lim_idx + 1 < len(limit_seq):
                            lim_idx += 1
                            params = dict(params)
                            params["limit"] = limit_seq[lim_idx]
                            logger.debug(f"[http] 503 fallback limit={params['limit']} params={params}")
                    jitter = random.uniform(0.4, 0.9)
                    sleep_s = backoff * jitter
                    backoff = min(backoff * 2.0, 8.0)
                    logger.debug(f"[http] retry {attempt}/{max_retries} status={status} sleep={sleep_s:.2f}")
                    await asyncio.sleep(sleep_s)
                    continue
                else:
                    # Non-retriable 4xx: skip
                    logger.warning(f"[http] non-retriable status={status} params={params} body[:200]={text[:200]!r}")
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            jitter = random.uniform(0.4, 0.9)
            sleep_s = backoff * jitter
            backoff = min(backoff * 2.0, 8.0)
            logger.debug(f"[http] exception {type(e).__name__} retry {attempt}/{max_retries} sleep={sleep_s:.2f} params={params}")
            await asyncio.sleep(sleep_s)
            continue

    #logger.warning(f"[http] {status} failure for params={params}")
    raise HttpRetryExhausted(f"status={status} offset={params['offset']}")

# ---------- Trade key + field extraction ----------

def extract_ts(row: Dict[str, Any]) -> Optional[int]:
    for k in ("timestamp", "time", "createdAt"):
        v = row.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    # try nested
    v = get_nested(row, "payload", "timestamp")
    if isinstance(v, int):
        return v
    return None

def extract_field(row: Dict[str, Any], keys: List[str], default=None):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    # nested common
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
        trade_uid, proxy_wallet, timestamp, condition_id, size, usdc_size,
        transaction_hash, price, asset, side, outcome_index
    )

def extract_market_meta_from_row(row: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    cond = str(extract_field(row, ["conditionId", "condition_id", "market"]) or "")
    slug = extract_field(row, ["slug"])
    desc = pick_first_nonempty(extract_field(row, ["title"]), extract_field(row, ["name"]))
    return cond, slug, desc

def extract_user_meta_from_rows(rows: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    name = None
    pseudonym = None
    for r in rows:
        # try top-level or nested 'user'
        name = name or pick_first_nonempty(
            str(extract_field(r, ["name"]) or ""),
            str(get_nested(r, "user", "name") or ""),
            str(get_nested(r, "user", "username") or ""),
        )
        pseudonym = pseudonym or pick_first_nonempty(
            str(extract_field(r, ["pseudonym"]) or ""),
            str(get_nested(r, "user", "pseudonym") or "")
        )
        if name and pseudonym:
            break
    return name, pseudonym

# ---------- DB Schema + operations ----------

def db_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)  # autocommit
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
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
    # markets (minimal)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS markets(
      condition_id TEXT PRIMARY KEY,
      slug TEXT,
      start_date TEXT,
      description TEXT
    );""")
    # users (minimal if absent)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
      id TEXT PRIMARY KEY,
      name TEXT,
      pseudonym TEXT
    );""")
    # Add columns if missing
    def col_exists(table: str, col: str) -> bool:
        cc = conn.execute(f"PRAGMA table_info({table});").fetchall()
        return any(c[1] == col for c in cc)
    if not col_exists("users", "name"):
        cur.execute("ALTER TABLE users ADD COLUMN name TEXT;")
    if not col_exists("users", "pseudonym"):
        cur.execute("ALTER TABLE users ADD COLUMN pseudonym TEXT;")
    cur.close()

def upsert_trades(conn: sqlite3.Connection, rows: List[Dict[str, Any]]) -> Tuple[int, set]:
    if not rows:
        return 0, set()
    cur = conn.cursor()
    conn.execute("BEGIN;")
    inserted = 0
    markets_seen: set = set()
    for r in rows:
        tpl = row_to_trade_tuple(r)
        markets_seen.add(tpl[3])  # condition_id
        try:
            cur.execute("""
                INSERT OR IGNORE INTO trades(
                    trade_uid, proxy_wallet, timestamp, condition_id, size, usdc_size,
                    transaction_hash, price, asset, side, outcome_index
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?);""", tpl)
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            # if schema mismatch happens, rollback that row and continue
            pass
    conn.execute("COMMIT;")
    cur.close()
    return inserted, markets_seen

def ensure_market_placeholders(conn: sqlite3.Connection, wallet: str, cond_to_meta: Dict[str, Tuple[Optional[str], Optional[str]]], logger: logging.Logger) -> int:
    if not cond_to_meta:
        return 0
    cur = conn.cursor()
    new_count = 0
    for cond, (slug, desc, oldest_ts) in cond_to_meta.items():
        if not cond:
            continue
        row = cur.execute("SELECT 1 FROM markets WHERE condition_id=?;", (cond,)).fetchone()
        if row:
            continue
        start_date_iso = utc_iso(oldest_ts) if oldest_ts else None
        cur.execute(
            "INSERT OR IGNORE INTO markets(condition_id, slug, start_date, description) VALUES (?,?,?,?);",
            (cond, slug, start_date_iso, desc),
        )
        if cur.rowcount > 0:
            new_count += 1
            #logger.debug(f"[new-market] user={wallet} market={cond} upserted placeholder")
    cur.close()
    return new_count

def update_user_metadata(conn: sqlite3.Connection, wallet: str, name: Optional[str], pseudonym: Optional[str]) -> None:
    if not wallet:
        return
    # ensure user exists
    conn.execute("INSERT OR IGNORE INTO users(id) VALUES (?);", (wallet,))
    # update non-empty if DB empty
    row = conn.execute("SELECT name, pseudonym FROM users WHERE id=?;", (wallet,)).fetchone()
    cur_name, cur_pseudo = row if row else (None, None)
    set_name = (name and not (cur_name and str(cur_name).strip()))
    set_pseudo = (pseudonym and not (cur_pseudo and str(cur_pseudo).strip()))
    if set_name or set_pseudo:
        conn.execute(
            "UPDATE users SET name=COALESCE(?,name), pseudonym=COALESCE(?,pseudonym) WHERE id=?;",
            (name if set_name else None, pseudonym if set_pseudo else None, wallet)
        )

def get_all_user_ids(conn: sqlite3.Connection) -> List[str]:
    try:
        rows = conn.execute("SELECT id FROM users;").fetchall()
        return [r[0] for r in rows if r and r[0]]
    except Exception:
        return []

# ---------- Fetch routines ----------

async def fetch_edge_ts(session, limiter, logger, user: str, asc: bool) -> Optional[int]:
    params = {
        "user": user,
        "limit": 1,
        "sortBy": "TIMESTAMP",
        "sortDirection": "ASC" if asc else "DESC",
    }
    data = await api_get_json(session, limiter, params, logger)
    if not data:
        return None
    return extract_ts(data[0])

async def fetch_page(session, limiter, logger, user: str, limit: int, offset: int, start: Optional[int], end: Optional[int]) -> List[Dict[str, Any]]:
    params = {
        "user": user,
        "limit": int(limit),
        "offset": int(offset),
        "sortBy": "TIMESTAMP",
        "sortDirection": "DESC",
    }
    if start is not None:
        params["start"] = int(start)
    if end is not None:
        params["end"] = int(end)
    data = await api_get_json(session, limiter, params, logger)
    if data is None:
        return []
    #logger.debug(f"[fetch] user={user} limit={limit} offset={offset} start={start} end={end} rows={len(data)}")
    return data

async def fetch_offsets_pass(session, limiter, logger, user: str, start_ts: int | None, end_ts: int | None) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    num_requests = 0
    for off in (0, 500, 1000):
        data = await fetch_page(session, limiter, logger, user, 500, off, start_ts, end_ts)
        num_requests += 1
        if not data:
            continue
        out.extend(data)

        if len(data) < 500 and off < 1000:
            # still try next offset? The spec says offsets 0/500/1000 always. We stop early to reduce load.
            break
    return out, num_requests

async def fetch_time_range_dc(
    session, limiter, logger, user: str,
    t0: int, t1: int, cap: int, seen: set
) -> tuple[list[dict], int]:
    """Fetch [t0, t1] by recursively halving when a window hits cap."""
    if t1 < t0:
        return [], 0

    # try whole window first
    rows, reqs = await fetch_offsets_pass(session, limiter, logger, user, t0, t1)
    if len(rows) < cap:
        # accept page; dedupe against global 'seen'
        out = []
        added = 0
        for r in rows:
            uid = compute_trade_uid(r)
            if uid in seen:
                continue
            seen.add(uid)
            out.append(r)
            added += 1
        #logger.debug(f"[dc] user={user} window={t0}->{t1} rows={len(rows)} new={added}")
        return out, reqs

    # hit cap → split in half
    mid = t0 + (t1 - t0) // 2
    left_rows, left_reqs = await fetch_time_range_dc(session, limiter, logger, user, t0, mid, cap, seen)
    right_rows, right_reqs = await fetch_time_range_dc(session, limiter, logger, user, mid + 1, t1, cap, seen)
    return left_rows + right_rows, reqs + left_reqs + right_reqs

async def fetch_user_all(session, limiter, logger, user: str) -> Tuple[List[Dict[str, Any]], int, bool]:
    """
    Returns (rows, num_requests, used_fallback)
    """
    rows: List[Dict[str, Any]] = []
    rows, num_requests = await fetch_offsets_pass(session, limiter, logger, user, None, None)
    used_fallback = False

    # Deduplicate early by trade_uid to avoid growing memory with repeats.
    seen = set()
    deduped: List[Dict[str, Any]] = []
    for r in rows:
        uid = compute_trade_uid(r)
        if uid in seen:
            continue
        seen.add(uid)
        deduped.append(r)
    rows = deduped

    full_pages = 3 if len(rows) >= 1500 else sum(1 for k in (0, 500, 1000))
    need_fallback = (len(rows) >= 1500)  # heuristic near-cap
    if not rows:
        return rows, num_requests, used_fallback

    oldest_ts = min(extract_ts(r) or now_utc_ts() for r in rows)
    current_end = (oldest_ts - 1) if oldest_ts else int(time.time())
    window_days = 30
    tw_curr = 1.0  # days; start small and ramp up
    min_days = 1/24
    cap = 1495
    oldest_edge: Optional[int] = None

    empty_after_data = 0

    if need_fallback:
        used_fallback = True
        oldest_edge = await fetch_edge_ts(session, limiter, logger, user, asc=True)
        t0 = max(0, int(oldest_edge or 0))
        t1 = int(time.time())
        mid = t0 + (t1 - t0) // 2

        # fetch first half, then second half; each half will recursively split again if it hits cap
        left_rows, left_reqs = await fetch_time_range_dc(session, limiter, logger, user, t0, mid, cap=1495, seen=seen)
        right_rows, right_reqs = await fetch_time_range_dc(session, limiter, logger, user, mid + 1, t1, cap=1495, seen=seen)

        rows.extend(left_rows)
        rows.extend(right_rows)
        num_requests += left_reqs + right_reqs

        #logger.debug(f"[fallback-dc] user={user} total_rows={len(rows)} request={left_reqs+right_reqs}")


    return rows, num_requests, used_fallback

# ---------- Raw I/O ----------

def raw_path_for_user(raw_trades_dir: Path, wallet: str) -> Path:
    a, b = _shard_parts(wallet)
    return raw_trades_dir / a / b / f"{wallet}.jsonl"

def write_raw_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(safe_json_dumps(r) + "\n")

def read_raw_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out

# ---------- Single-writer worker ----------

class IngestWorker:
    def __init__(self, db_path: Path, logger: logging.Logger):
        self.db_path = db_path
        self.logger = logger
        self.queue: asyncio.Queue = asyncio.Queue()
        self._stop = asyncio.Event()
        self._conn: Optional[sqlite3.Connection] = None
        self.total_inserted = 0
        self.total_files = 0
        self.total_placeholders = 0

    async def start(self):
        # Open connection in main thread; we will only use it sequentially in this worker.
        self._conn = db_connect(self.db_path)
        ensure_tables(self._conn)

    async def stop(self):
        self._stop.set()
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    async def put(self, wallet: str, filepath: Path, name: Optional[str], pseudonym: Optional[str]):
        await self.queue.put((wallet, filepath, name, pseudonym))

    async def run(self):
        assert self._conn is not None
        while not (self._stop.is_set() and self.queue.empty()):
            try:
                wallet, filepath, name, pseudonym = await asyncio.wait_for(self.queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            try:
                rows = await asyncio.to_thread(read_raw_jsonl, filepath)
                # Insert trades
                inserted, markets = await asyncio.to_thread(upsert_trades, self._conn, rows)
                # Update user metadata
                await asyncio.to_thread(update_user_metadata, self._conn, wallet, name, pseudonym)
                # Ensure market placeholders
                # Compute per-condition oldest ts + meta from rows
                cond_meta: Dict[str, Tuple[Optional[str], Optional[str], Optional[int]]] = {}
                for r in rows:
                    cond, slug, desc = extract_market_meta_from_row(r)
                    ts = extract_ts(r)
                    if cond not in cond_meta:
                        cond_meta[cond] = (slug, desc, ts)
                    else:
                        cur = cond_meta[cond]
                        # track oldest ts
                        ots = ts if ts is not None else cur[2]
                        if cur[2] is not None and ts is not None:
                            ots = min(cur[2], ts)
                        cond_meta[cond] = (cur[0] or slug, cur[1] or desc, ots)
                placeholders = await asyncio.to_thread(
                    ensure_market_placeholders,
                    self._conn,
                    wallet,
                    cond_meta,
                    self.logger
                )
                self.total_inserted += inserted
                self.total_files += 1
                self.total_placeholders += placeholders
                #self.logger.debug(f"[ingest] user={wallet} file={filepath.name} inserted={inserted} placeholders={placeholders}")
            except Exception as e:
                self.logger.warning(f"[ingest] failed wallet={wallet} file={filepath} err={type(e).__name__}: {e}")
            finally:
                self.queue.task_done()

# ---------- Orchestrator ----------

# signature
async def process_user(
    user: str,
    session: aiohttp.ClientSession,
    limiter: SlidingWindowLimiter,
    logger: logging.Logger,
    raw_trades_dir: Path,
    resume: bool,
    ingest: IngestWorker,
    sem: asyncio.Semaphore,
    stats: Dict[str, Any],
    stats_lock: asyncio.Lock,
    log_every: int,
):

    async with sem:
        t0 = time.monotonic()
        raw_path = raw_path_for_user(raw_trades_dir, user)
        if resume and raw_path.exists() and raw_path.stat().st_size:
            # enqueue ingestion of existing file
            rows = read_raw_jsonl(raw_path)
            name, pseudo = extract_user_meta_from_rows(rows)
            await ingest.put(user, raw_path, name, pseudo)
            dt = time.monotonic() - t0

            async with stats_lock:
                stats["skipped"] += 1
                stats["processed"] += 1
                if stats["processed"] % log_every == 0:
                    logger.info(
                        f"[progress] processed={stats['processed']} fetched={stats['fetched']} skipped={stats['skipped']} failed={stats['failed']} "
                        f"rows_total={stats['rows']} requests={stats['requests']} fallback_users={stats['fallback_users']}"
                    )
                    stats['requests'] = stats['fallback_users'] = stats['rows'] = stats['failed'] = stats['skipped'] = stats['fetched'] = 0
            return

        try:
            rows, num_requests, used_fallback = await fetch_user_all(session, limiter, logger, user)
        except HttpRetryExhausted as e:
            logger.error(f"[skip-user] retries exhausted for user={user}: {e}")
            rows, num_requests, used_fallback = [], 0, False
        except Exception as e:
            logger.error(f"[skip-user] unexpected error for user={user}: {e}")
            rows, num_requests, used_fallback = [], 0, False


        if used_fallback:
            #logger.debug(f"[fallback-summary] user={user} total_rows={len(rows)} requests={num_requests}")
            pass

        # Write raw file
        write_raw_jsonl(raw_path, rows)
        # Extract user meta
        name, pseudo = extract_user_meta_from_rows(rows)
        # Enqueue for ingestion
        await ingest.put(user, raw_path, name, pseudo)

        dt = time.monotonic() - t0

        async with stats_lock:
            stats["fetched"] += 1
            stats["rows"] += len(rows)
            stats["requests"] += num_requests
            if used_fallback:
                stats["fallback_users"] += 1
            stats["processed"] += 1
            if stats["processed"] % log_every == 0:
                logger.info(
                    f"[progress] processed={stats['processed']} fetched={stats['fetched']} skipped={stats['skipped']} failed={stats['failed']} "
                    f"rows={stats['rows']} requests={stats['requests']} fallback_users={stats['fallback_users']}"
                )
                stats['requests'] = stats['fallback_users'] = stats['rows'] = stats['failed'] = stats['skipped'] = stats['fetched'] = 0

# ---------- Main ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch Polymarket user trades → per-user JSONL → single-writer SQLite ingest.")
    p.add_argument("--only", type=str, help="Comma-separated list of user wallets to limit to", default=None)
    p.add_argument("--resume", action="store_true", help="Skip fetching for users whose raw JSONL exists in this batch; enqueue ingest.")
    return p.parse_args()

async def main_async():
    # Base paths
    base_dir = Path(BASE_DIR)
    ensure_dir(base_dir)

    # Batch + logging
    batch_tag = ts_to_batch(now_utc_ts())
    log_level = env_str("PMDB_LOG_LEVEL", "INFO")
    logger = setup_logger(base_dir, batch_tag, log_level)

    # Env knobs
    concurrency = max(1, env_int("PMDB_CONCURRENCY", 15))
    rate_max = max(1, env_int("PMDB_RATE_MAX", 150))
    rate_win = max(1, env_int("PMDB_RATE_WINDOW", 10))
    raw_dir_name = env_str("PMDB_RAW_DIR", "raw_trades")
    users_limit = max(0, env_int("PMDB_USERS_LIMIT", 0))

    concurrency = 15
    rate_max = 150
    rate_win = 10

    db_path = "C:/Users/nimro/polymarket-db-C/polymarket.db"
    raw_trades_dir = base_dir / raw_dir_name
    ensure_dir(raw_trades_dir)

    args = parse_args()

    # Determine users list
    only_list: Optional[List[str]] = None
    if args.only:
        only_list = [w.strip() for w in args.only.split(",") if w.strip()]

    # Prepare DB connection for reading users or creating if missing
    rd_conn = db_connect(db_path)
    ensure_tables(rd_conn)

    if only_list:
        users = only_list
    else:
        users = get_all_user_ids(rd_conn)
        if users_limit > 0:
            users = users[:users_limit]
    if not users:
        print("No users to process. Provide --only or ensure users table is populated.", file=sys.stderr)
        return

    # Start ingest worker
    ingest = IngestWorker(db_path, logger)
    await ingest.start()
    worker_task = asyncio.create_task(ingest.run())

    limiter = SlidingWindowLimiter(rate_max, rate_win, logger)
    sem = asyncio.Semaphore(concurrency)

    # HTTP session
    timeout = aiohttp.ClientTimeout(total=30)
    conn = aiohttp.TCPConnector(limit=concurrency * 4, force_close=False, ttl_dns_cache=60)
    async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
        logger.info(
            f"[start] db={db_path} raw_dir={raw_trades_dir} concurrency={concurrency} rate={rate_max}/{rate_win}s users={len(users)} resume={args.resume}"
        )

        LOG_EVERY = max(1, env_int("PMDB_LOG_EVERY", 1000))
        stats_lock = asyncio.Lock()
        stats = {"fetched": 0, "skipped": 0, "rows": 0, "requests": 0, "fallback_users": 0, "processed": 0, "failed": 0}

        tasks = [
            process_user(u, session, limiter, logger, raw_trades_dir, args.resume, ingest, sem, stats, stats_lock, LOG_EVERY)
            for u in users
        ]

        await asyncio.gather(*tasks)

        # drain queue
        await ingest.queue.join()
        await ingest.stop()
        # stop worker
        await asyncio.sleep(0.05)  # allow graceful stop condition
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass

        logger.info(
            f"[done]"
        )

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
