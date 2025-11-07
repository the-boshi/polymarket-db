# Async fetcher for many takers with a strict 150/10s limit.
# Parallel across takers. Notebook-safe blocking wrapper.

import asyncio, contextlib, random, json, re, logging
from collections import deque
from typing import List, Dict, Optional
import aiohttp
import nest_asyncio
import pandas as pd
from aiohttp import ContentTypeError

from get_all_takers import get_all_takers
from utils import parquet_num_rows

nest_asyncio.apply()

API = "https://data-api.polymarket.com/trades"

# Rate/limits
MAX_CALLS = 150          # max requests in any rolling window
WINDOW_S = 10.0          # window seconds
RPS_BASE = 12.0          # baseline steady rate (< 150/10s)
MAX_CONCURRENCY = 60     # sockets/tasks
MAX_RETRIES = 7          # per request

DEFAULT_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "polymarket-db-fetcher/0.1 (+github.com/the-boshi/polymarket-db)"
}

# ------------------------------------------------------------
# Logging helpers
# ------------------------------------------------------------

logger = logging.getLogger("fetch")

def clean_html_snippet(text: str, max_len: int = 200) -> str:
    """Strip tags, remove script blocks, collapse whitespace, cut before inline JS."""
    text = re.sub(r"(?is)<script.*?</script>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    m = re.search(r"\(function|\bfunction\s*\(|\bvar\s|\bwindow\.|\bdocument\.", text, re.I)
    if m:
        text = text[:m.start()].rstrip()
    return text[:max_len]

def _pview(p: dict) -> Dict[str, Optional[str]]:
    return {k: p.get(k) for k in ("user", "market", "offset", "limit")}

# ------------------------------------------------------------
# Strict global sliding-window limiter (singleton)
# ------------------------------------------------------------

class GlobalRateLimiter:
    """Hard cap: <= max_calls within any rolling 'window' seconds.
       Also enforces a minimum interval and a global cooldown after 429."""
    def __init__(self, max_calls=MAX_CALLS, window=WINDOW_S, min_rps=RPS_BASE):
        self.max_calls = max_calls
        self.window = window
        self.min_interval = 1.0 / min_rps
        self._times = deque()          # timestamps of recent requests
        self._lock = asyncio.Lock()
        self.cooldown_until = 0.0

    async def acquire(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            while True:
                now = loop.time()

                # Global cooldown
                if now < self.cooldown_until:
                    await asyncio.sleep(self.cooldown_until - now)
                    continue

                # Drop old entries
                while self._times and (now - self._times[0]) > self.window:
                    self._times.popleft()

                # Strict window
                if len(self._times) >= self.max_calls:
                    sleep_for = self.window - (now - self._times[0]) + random.random() * 0.02
                    await asyncio.sleep(max(0.01, sleep_for))
                    continue

                # Soft smoothing between requests
                if self._times:
                    since_last = now - self._times[-1]
                    if since_last < self.min_interval:
                        await asyncio.sleep(self.min_interval - since_last + random.random() * 0.01)
                        continue

                # Allow request
                self._times.append(loop.time())
                return

    def apply_429(self, retry_after: float):
        now = asyncio.get_event_loop().time()
        self.cooldown_until = max(self.cooldown_until, now + max(1.0, retry_after))
        # Slow down spacing by 25%, but not below 5 rps (interval <= 1s)
        self.min_interval = min(max(self.min_interval * 1.25, 1.0 / 5.0), 1.0)

_RATE = GlobalRateLimiter(max_calls=MAX_CALLS, window=WINDOW_S, min_rps=RPS_BASE)
def get_rate() -> GlobalRateLimiter:
    return _RATE

# ------------------------------------------------------------
# HTTP core
# ------------------------------------------------------------

async def _get_json(session: aiohttp.ClientSession, params: dict):
    rate = get_rate()
    backoff = 0.5
    last_status = None
    last_body = None

    for attempt in range(MAX_RETRIES):
        await rate.acquire()
        await asyncio.sleep(random.random() * 0.02)  # jitter

        try:
            async with session.get(API, params=params, timeout=60) as r:
                last_status = r.status

                if 200 <= r.status < 300:
                    try:
                        return await r.json()
                    except (ContentTypeError, json.JSONDecodeError):
                        text = await r.text()
                        raise RuntimeError(f"Bad JSON decode status={r.status} body[:200]={text[:200]!r}")

                # Retryable
                if r.status in (408, 429, 500, 502, 503, 504):
                    with contextlib.suppress(Exception):
                        raw = await r.text()
                        last_body = clean_html_snippet(raw)

                    ra = r.headers.get("Retry-After")
                    delay = float(ra) if ra and ra.replace(".", "", 1).isdigit() \
                           else backoff + random.random() * 0.5

                    if r.status == 429:
                        rate.apply_429(delay)
                        logger.warning(
                            f"[fetch] 429 attempt {attempt+1}/{MAX_RETRIES} "
                            f"params={_pview(params)} delay={delay:.2f}s msg={last_body!r}"
                        )
                        backoff = min(backoff * 2.5, 16.0)
                    else:
                        logger.warning(
                            f"[fetch] Retryable HTTP {r.status} attempt {attempt+1}/{MAX_RETRIES} "
                            f"params={_pview(params)} delay={delay:.2f}s msg={last_body!r}"
                        )
                        backoff = min(backoff * 2.0, 12.0)

                    await asyncio.sleep(delay)
                    continue

                # Non-retryable
                body = await r.text()
                raise RuntimeError(f"HTTP {r.status}: body[:200]={body[:200]!r}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_body = str(e)
            delay = backoff + random.random() * 0.5
            logger.warning(
                f"[fetch] Network {type(e).__name__} attempt {attempt+1}/{MAX_RETRIES} "
                f"params={_pview(params)} delay={delay:.2f}s err={last_body}"
            )
            await asyncio.sleep(delay)
            backoff = min(backoff * 2.0, 12.0)

    p = _pview(params)
    raise RuntimeError(
        f"GET failed after retries status={last_status} body[:200]={last_body!r} params={p}"
    )

# ------------------------------------------------------------
# Pagination: 25/page up to max_offset
# ------------------------------------------------------------

async def fetch_trades_for_user_market_async(session: aiohttp.ClientSession,
                                             user_addr: str,
                                             condition_id: str,
                                             page_size: int = 25,
                                             max_offset: int = 1000) -> List[dict]:
    out, offset = [], 0
    while True:
        params = {"user": user_addr, "market": condition_id, "limit": page_size, "offset": offset}
        batch = await _get_json(session, params)
        if not batch:
            break
        out.extend(batch)
        if len(batch) != page_size:
            break
        offset += page_size
        if offset > max_offset:
            break
    return out

# ------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------

async def get_trades_for_market_async(condition_id, clobs) -> List[dict]:
    takers = get_all_takers(clobs)
    logger.info(f"Got {len(takers)} taker addresses.")

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, ttl_dns_cache=300)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: List[dict] = []

    async with aiohttp.ClientSession(connector=connector, headers=DEFAULT_HEADERS) as session:
        async def worker(addr: str):
            async with sem:
                trades = await fetch_trades_for_user_market_async(session, addr, condition_id)
                if trades:
                    results.extend(trades)

        # optional: waves to smooth further
        TASK_BATCH = 100
        tasks: List[asyncio.Task] = []
        for i in range(0, len(takers), TASK_BATCH):
            batch = takers[i:i+TASK_BATCH]
            tasks = [asyncio.create_task(worker(a)) for a in batch]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.3)  # tiny gap between waves

    return results

def get_trades_for_market(condition_id, clobs):
    # notebook-safe: nest_asyncio is applied above
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_trades_for_market_async(condition_id, clobs))

def get_trades_save_parquet(condition_id, clobs, file_path, skip_existing):
    file_rows = parquet_num_rows(file_path)
    need_rewrite = (file_rows <= 0)
    if skip_existing and not need_rewrite:
        return 0
    trades = get_trades_for_market(condition_id, clobs)

    df = pd.json_normalize(trades, sep=".")
    df.to_parquet(file_path, index=False)
    return 1
