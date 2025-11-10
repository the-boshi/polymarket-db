# fetch_trades.py
# Fetch all Polymarket activity trades per user and upsert into SQLite.
# Stage 1: async fetch → raw JSONL per user (rate-limited, concurrent).
# Stage 2: sync upsert → SQLite (creates/updates schema as needed).
#
# Env knobs:
#   PMDB_LOG_LEVEL=DEBUG|INFO|WARNING
#   PMDB_CONCURRENCY=8          # concurrent users
#   PMDB_RATE_MAX=100           # max requests per window
#   PMDB_RATE_WINDOW=10         # window seconds
#   PMDB_RAW_DIR=raw_trades     # folder under BASE_DIR
#   PMDB_USERS_LIMIT=0          # 0 means all users; else first N
#
# Usage:
#   python fetch_trades.py
#   python fetch_trades.py --only 0xabc,0xdef     # subset of users
#   python fetch_trades.py --resume               # skip users with existing raw file

import os, json, time, asyncio, logging, hashlib, sqlite3
import argparse
from typing import Dict, List, Optional, Tuple, Set
from collections import deque
from datetime import datetime

import aiohttp

# -------------------------
# Paths and constants
# -------------------------
BASE_DIR   = os.path.expanduser("~/Documents/Projects/polymarket-db")
DB_PATH    = os.path.join(BASE_DIR, "polymarket_copy.db")
RAW_DIR    = os.path.join(BASE_DIR, os.getenv("PMDB_RAW_DIR", "raw_trades"))
LOG_DIR    = os.path.join(BASE_DIR, "logs")
LOG_PATH   = os.path.join(LOG_DIR, f"trades-{time.strftime('%Y%m%d-%H%M%S')}.log")

LOG_LEVEL  = os.getenv("PMDB_LOG_LEVEL", "INFO").upper()
CONCURRENCY = int(os.getenv("PMDB_CONCURRENCY", os.getenv("PMDB_CONCURRENCY", os.getenv("PMDB_CONCURRENCY", "8"))))
RATE_MAX   = int(os.getenv("PMDB_RATE_MAX", "100"))
RATE_WIN   = float(os.getenv("PMDB_RATE_WINDOW", "10"))
USERS_LIMIT = int(os.getenv("PMDB_USERS_LIMIT", "0"))

API_URL = "https://data-api.polymarket.com/activity"
HEADERS = {"User-Agent": "polymarket-db/1.0", "Accept": "application/json"}

# Paging and fallback thresholds
PAGE_SIZE = 500
OFFSET_STEPS = (0, 500, 1000)             # offset+limit <= 1500
NEAR_CAP_THRESHOLD = 1400                 # switch to time-window if >= this
INIT_WINDOW_DAYS = 30                     # initial time window
MIN_WINDOW_DAYS  = 1
EMPTY_WINDOWS_TO_STOP = 2                 # stop after this many empty windows after seeing data

# -------------------------
# Logger
# -------------------------
def get_logger() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("polymarket_trades")
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logger.level); fh.setFormatter(fmt); logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logger.level); sh.setFormatter(fmt); logger.addHandler(sh)
    return logger

logger = get_logger()

# -------------------------
# Rate limiter: sliding window (N per T seconds) across all tasks
# -------------------------
class SlidingWindowLimiter:
    def __init__(self, max_calls: int, per_seconds: float):
        self.max_calls = max_calls
        self.per = per_seconds
        self._dq = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            while self._dq and (now - self._dq[0]) > self.per:
                self._dq.popleft()
            if len(self._dq) >= self.max_calls:
                sleep_for = self.per - (now - self._dq[0]) + 0.01
                await asyncio.sleep(sleep_for)
                # re-evaluate after sleep
                return await self.acquire()
            self._dq.append(time.monotonic())

# -------------------------
# SQLite schema and helpers
# -------------------------
TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
  trade_uid TEXT PRIMARY KEY,
  proxy_wallet TEXT NOT NULL,
  timestamp INTEGER NOT NULL,
  condition_id TEXT,
  size REAL,
  usdc_size REAL,
  transaction_hash TEXT,
  price REAL,
  asset TEXT,
  side TEXT,
  outcome_index INTEGER
);
CREATE INDEX IF NOT EXISTS idx_trades_user_time ON trades(proxy_wallet, timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(condition_id);
"""

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(TRADES_SCHEMA)
    # users extra cols if missing
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cur.fetchall()}
    if "name" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN name TEXT")
    if "pseudonym" not in cols:
        conn.execute("ALTER TABLE users ADD COLUMN pseudonym TEXT")
    conn.commit()

def update_user_metadata(conn: sqlite3.Connection, user: str, name: Optional[str], pseudonym: Optional[str]):
    if not name and not pseudonym:
        return
    # only fill if NULL or empty
    cur = conn.cursor()
    cur.execute("SELECT name, pseudonym FROM users WHERE id=?", (user,))
    row = cur.fetchone()
    if row is None:
        return
    cur_name, cur_pseudo = row
    name_to_set = name if name and not cur_name else None
    pseudo_to_set = pseudonym if pseudonym and not cur_pseudo else None
    if name_to_set or pseudo_to_set:
        conn.execute(
            "UPDATE users SET name=COALESCE(?, name), pseudonym=COALESCE(?, pseudonym) WHERE id=?",
            (name_to_set, pseudo_to_set, user),
        )

def trade_uid(t: Dict) -> str:
    # Stable digest across retries and overlapping windows
    key = "|".join([
        str(t.get("proxyWallet","")),
        str(t.get("timestamp","")),
        str(t.get("conditionId","")),
        str(t.get("asset","")),
        str(t.get("side","")),
        # normalize floats to fixed repr
        f'{float(t.get("price", 0.0)):.10f}',
        f'{float(t.get("size", 0.0)):.10f}',
        str(t.get("transactionHash","")),
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()

def upsert_trades(conn: sqlite3.Connection, user: str, rows: List[Dict]) -> int:
    if not rows:
        return 0
    cur = conn.cursor()
    n = 0
    for r in rows:
        # NEW: ensure market exists and warn once per missing market
        ensure_market_exists(conn, r, user)

        uid = trade_uid(r)
        cur.execute(
            """INSERT OR IGNORE INTO trades(
                 trade_uid, proxy_wallet, timestamp, condition_id, size, usdc_size,
                 transaction_hash, price, asset, side, outcome_index
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                uid,
                r.get("proxyWallet"),
                int(r.get("timestamp")),
                r.get("conditionId"),
                float(r.get("size") or 0.0),
                float(r.get("usdcSize") or 0.0),
                r.get("transactionHash"),
                float(r.get("price") or 0.0),
                r.get("asset"),
                r.get("side"),
                int(r.get("outcomeIndex") or 0),
            ),
        )
        n += cur.rowcount
    return n

# -------------------------
# Fetching logic
# -------------------------
RETRY_STATUS = {408, 409, 420, 429, 500, 502, 503, 504}

async def _get(session: aiohttp.ClientSession, limiter: SlidingWindowLimiter, params: Dict, max_retries: int = 7) -> Optional[List[Dict]]:
    backoff = 0.5
    last_status = None
    last_body = None
    for attempt in range(max_retries):
        await limiter.acquire()
        try:
            async with session.get(API_URL, params=params, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                last_status = resp.status
                if resp.status in RETRY_STATUS:
                    last_body = (await resp.text())[:200]
                    ra = resp.headers.get("Retry-After")
                    try:
                        delay = float(ra) if ra else backoff
                    except ValueError:
                        delay = backoff
                    logger.debug(f"[get] {resp.status} retry in {delay:.2f}s params={params}")
                    await asyncio.sleep(delay)
                    backoff = min(backoff * 1.8, 8.0)
                    continue
                if resp.status >= 400:
                    last_body = (await resp.text())[:200]
                    logger.warning(f"[get] non-retryable HTTP {resp.status} body={last_body!r} params={params}")
                    return None
                data = await resp.json()
                if isinstance(data, list):
                    return data
                # Some deployments wrap in object; normalize
                if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
                    return data["data"]
                return data if isinstance(data, list) else []
        except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
            logger.debug(f"[get] network error {e} backoff={backoff:.2f}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.8, 8.0)
    logger.warning(f"[get] failed after retries status={last_status} body={last_body!r} params={params}")
    return None

async def fetch_offset_span(session: aiohttp.ClientSession, limiter: SlidingWindowLimiter, user: str) -> List[Dict]:
    rows: List[Dict] = []
    for off in OFFSET_STEPS:
        params = {
            "limit": str(PAGE_SIZE),
            "offset": str(off),
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "user": user,
        }
        page = await _get(session, limiter, params)
        if not page:
            break
        rows.extend(page)
        logger.debug(f"[offset] user={user} off={off} got={len(page)} acc={len(rows)}")
        if len(page) < PAGE_SIZE:
            break
    return rows

async def fetch_window_pages(session: aiohttp.ClientSession, limiter: SlidingWindowLimiter, user: str, start_ts: int, end_ts: int) -> Tuple[List[Dict], bool]:
    """Return rows, hit_near_cap"""
    rows: List[Dict] = []
    for off in OFFSET_STEPS:
        params = {
            "limit": str(PAGE_SIZE),
            "offset": str(off),
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
            "user": user,
            "start": str(start_ts),
            "end": str(end_ts),
        }
        page = await _get(session, limiter, params)
        if not page:
            break
        rows.extend(page)
        logger.debug(f"[window] user={user} {start_ts}->{end_ts} off={off} got={len(page)} acc={len(rows)}")
        if len(page) < PAGE_SIZE:
            break
    return rows, (len(rows) >= NEAR_CAP_THRESHOLD)

def _dedup_rows(rows: List[Dict]) -> List[Dict]:
    seen: Set[str] = set()
    out: List[Dict] = []
    for r in rows:
        uid = trade_uid(r)
        if uid in seen:
            continue
        seen.add(uid)
        out.append(r)
    return out

async def fetch_user_all(session: aiohttp.ClientSession, limiter: SlidingWindowLimiter, user: str) -> Tuple[List[Dict], bool, Optional[str], Optional[str]]:
    """
    Returns: (rows, used_windows, name, pseudonym)
    """
    # First pass: offset span up to 1500
    rows = await fetch_offset_span(session, limiter, user)
    used_windows = False

    # Name/pseudonym from first page if present
    name = None
    pseudo = None
    for r in rows[:50]:
        name = name or r.get("name")
        pseudo = pseudo or r.get("pseudonym")
        if name and pseudo:
            break

    if len(rows) >= NEAR_CAP_THRESHOLD:
        used_windows = True
        # continue backwards with time windows
        rows = _dedup_rows(rows)
        if rows:
            oldest = min(int(r.get("timestamp") or 0) for r in rows)
            end_ts = oldest - 1
        else:
            end_ts = int(time.time())
        window_days = INIT_WINDOW_DAYS
        empty_streak = 0
        got_any = False
        total_before_windows = len(rows)

        while True:
            start_ts = end_ts - int(window_days * 86400)
            w_rows, near_cap = await fetch_window_pages(session, limiter, user, start_ts, end_ts)
            if near_cap and window_days > MIN_WINDOW_DAYS:
                # shrink and re-try same end
                window_days = max(MIN_WINDOW_DAYS, max(1, window_days // 2))
                logger.debug(f"[window] user={user} near-cap, shrink window_days={window_days}")
                continue

            if not w_rows:
                empty_streak += 1
                if got_any and empty_streak >= EMPTY_WINDOWS_TO_STOP:
                    logger.debug(f"[window] user={user} two empty windows, stop.")
                    break
            else:
                got_any = True
                empty_streak = 0
                rows.extend(w_rows)
                rows = _dedup_rows(rows)

            end_ts = start_ts - 1
            # safety: stop if we went too far back
            if end_ts < 1_500_000_000:  # ~2017
                break

        logger.warning(f"[user] time-window fallback: user={user} total_rows={len(rows)} (initial={total_before_windows})")

    return _dedup_rows(rows), used_windows, name, pseudo

# -------------------------
# Raw write and ingest
# -------------------------
def user_raw_path(batch_dir: str, user: str) -> str:
    safe = user.lower()
    return os.path.join(batch_dir, f"{safe}.jsonl")

def write_raw_jsonl(batch_dir: str, user: str, rows: List[Dict]) -> str:
    os.makedirs(batch_dir, exist_ok=True)
    fp = user_raw_path(batch_dir, user)
    with open(fp, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return fp

def read_raw_jsonl(path: str) -> List[Dict]:
    out: List[Dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                out.append(json.loads(s))
            except json.JSONDecodeError:
                continue
    return out

def ensure_market_exists(conn: sqlite3.Connection, r: Dict, user: str) -> None:
    """If a trade references a market not in `markets`, insert a minimal placeholder and log a warning."""
    cid = r.get("conditionId")
    if not cid:
        return
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM markets WHERE condition_id=?", (cid,))
    if cur.fetchone():
        return

    slug = r.get("slug")
    desc = r.get("title") or r.get("name")
    ts = r.get("timestamp")
    start_iso = None
    try:
        start_iso = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        pass

    cur.execute(
        """INSERT OR IGNORE INTO markets
           (condition_id, slug, start_date, end_date, description, volume, closed, event_id, winner_asset_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (cid, slug, start_iso, None, desc, None, None, None, None),
    )
    logger.warning(f"[new-market] user={user} market={cid} encountered in trades; upserted placeholder row")


# -------------------------
# Orchestration
# -------------------------
async def fetch_all_users(users: List[str], batch_dir: str, concurrency: int, resume: bool) -> Dict[str, Tuple[int, bool, Optional[str], Optional[str]]]:
    limiter = SlidingWindowLimiter(RATE_MAX, RATE_WIN)
    sem = asyncio.Semaphore(concurrency)
    results: Dict[str, Tuple[int, bool, Optional[str], Optional[str]]] = {}

    connector = aiohttp.TCPConnector(limit=concurrency * 4)
    async with aiohttp.ClientSession(connector=connector) as session:
        async def one(user: str):
            fp = user_raw_path(batch_dir, user)
            if resume and os.path.isfile(fp):
                logger.info(f"[skip] resume is on and file exists: {fp}")
                rows = read_raw_jsonl(fp)
                results[user] = (len(rows), False, None, None)
                return
            async with sem:
                t0 = time.time()
                rows, used_windows, name, pseudo = await fetch_user_all(session, limiter, user)
                write_raw_jsonl(batch_dir, user, rows)
                dt = time.time() - t0
                results[user] = (len(rows), used_windows, name, pseudo)
                logger.info(f"[done] user={user} rows={len(rows)} used_windows={used_windows} t={dt:.1f}s")

        tasks = [asyncio.create_task(one(u)) for u in users]
        for coro in asyncio.as_completed(tasks):
            await coro

    return results

def list_users(conn: sqlite3.Connection, only: Optional[List[str]], limit: int) -> List[str]:
    cur = conn.cursor()
    if only:
        qmarks = ",".join("?" for _ in only)
        cur.execute(f"SELECT id FROM users WHERE id IN ({qmarks})", only)
        users = [r[0] for r in cur.fetchall()]
    else:
        cur.execute("SELECT id FROM users")
        users = [r[0] for r in cur.fetchall()]
    users = [u for u in users if u]  # clean
    users.sort()
    if limit and limit > 0:
        users = users[:limit]
    return users

def ingest_batch(conn: sqlite3.Connection, batch_dir: str, fetch_meta: Dict[str, Tuple[int, bool, Optional[str], Optional[str]]]) -> Tuple[int, int]:
    files = [user_raw_path(batch_dir, u) for u in fetch_meta.keys() if os.path.isfile(user_raw_path(batch_dir, u))]
    total_rows = 0
    total_new = 0
    with conn:
        for fp in files:
            rows = read_raw_jsonl(fp)
            total_rows += len(rows)
            # update user metadata based on this file
            user = os.path.splitext(os.path.basename(fp))[0]
            name = fetch_meta.get(user, (0, False, None, None))[2]
            pseudo = fetch_meta.get(user, (0, False, None, None))[3]
            update_user_metadata(conn, user, name, pseudo)
            n = upsert_trades(conn, user, rows)
            total_new += n
    return total_rows, total_new

def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="Comma-separated list of user proxyWallets to fetch", default=None)
    ap.add_argument("--resume", action="store_true", help="Skip users that already have a raw JSONL file in this batch dir")
    args = ap.parse_args()

    logger.info(f"Start. DB={DB_PATH} RAW_DIR={RAW_DIR} CONCURRENCY={CONCURRENCY} RATE={RATE_MAX}/{RATE_WIN}s LOG={LOG_PATH}")

    conn = sqlite3.connect(DB_PATH)
    ensure_schema(conn)

    only_list = [s.strip().lower() for s in args.only.split(",")] if args.only else None
    users = list_users(conn, only_list, USERS_LIMIT)
    if not users:
        logger.info("No users found.")
        return

    batch_dir = os.path.join(RAW_DIR, time.strftime("%Y%m%d-%H%M%S"))
    logger.info(f"Users to fetch: {len(users)} | batch_dir={batch_dir}")

    # Stage 1: fetch raw
    t0 = time.time()
    fetch_meta = asyncio.run(fetch_all_users(users, batch_dir, CONCURRENCY, resume=args.resume))
    dt1 = time.time() - t0

    # Warn users that hit time-window
    for u, (cnt, used_windows, _, _) in fetch_meta.items():
        if used_windows:
            logger.warning(f"[warn] user={u} triggered time-window fallback; rows={cnt}")

    # Stage 2: ingest
    t1 = time.time()
    total_rows, total_new = ingest_batch(conn, batch_dir, fetch_meta)
    dt2 = time.time() - t1
    conn.commit()
    conn.close()

    logger.info(f"Done. fetched_rows={total_rows} inserted_new={total_new} users={len(fetch_meta)} fetch_time={dt1:.1f}s ingest_time={dt2:.1f}s")

if __name__ == "__main__":
    main()
