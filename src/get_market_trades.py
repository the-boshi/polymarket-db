# One-cell async fetcher for many takers with precise 15 rps rate limit (150 per 10s).
# Assumes one page per taker. Parallel across takers. Notebook-safe blocking wrapper.

import asyncio, time, contextlib
from typing import Dict, List
import aiohttp
import nest_asyncio
import pandas as pd
from collections.abc import Mapping
import random
from aiohttp import ContentTypeError
import json

from get_all_takers import get_all_takers
from utils import parquet_num_rows

nest_asyncio.apply()

API = "https://data-api.polymarket.com/trades"
RPS = 13                    # 150 / 10s
CAPACITY = 150              # burst cap
MAX_CONCURRENCY = 200       # sockets/tasks; the bucket enforces the true rate

class TokenBucket:
    def __init__(self, rate_per_sec: float = RPS, capacity: int = CAPACITY):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.last = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
            self.last = now
            while self.tokens < 1:
                need = (1 - self.tokens) / self.rate
                await asyncio.sleep(need)
                now = asyncio.get_event_loop().time()
                self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate)
                self.last = now
            self.tokens -= 1

# requires: import random, contextlib, json
# from aiohttp import ContentTypeError

async def _get_json(session: aiohttp.ClientSession, bucket: TokenBucket, params: dict):
    backoff = 0.5
    last_status = None
    last_body = None

    for attempt in range(7):
        await bucket.acquire()
        await asyncio.sleep(random.random() * 0.05)  # jitter

        try:
            async with session.get(API, params=params, timeout=60) as r:
                last_status = r.status

                if 200 <= r.status < 300:
                    try:
                        return await r.json()
                    except (ContentTypeError, json.JSONDecodeError):
                        text = await r.text()
                        raise RuntimeError(f"Bad JSON decode status={r.status} body[:200]={text[:200]!r}")

                if r.status in (408, 429, 500, 502, 503, 504):
                    with contextlib.suppress(Exception):
                        last_body = (await r.text())[:200]
                    ra = r.headers.get("Retry-After")
                    delay = max(0.5, float(ra)) if ra and ra.replace(".", "", 1).isdigit() \
                            else backoff + random.random() * 0.5
                    await asyncio.sleep(delay)
                    backoff = min(backoff * 2, 8.0)
                    continue

                body = await r.text()
                raise RuntimeError(f"HTTP {r.status}: body[:200]={body[:200]!r}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_body = str(e)
            await asyncio.sleep(backoff + random.random() * 0.5)
            backoff = min(backoff * 2, 8.0)

    p = {k: params.get(k) for k in ("user", "market", "offset", "limit")}
    raise RuntimeError(
        f"GET failed after retries status={last_status} body[:200]={last_body!r} params={p}"
    )

# paginate with page_size=25; continue only when exactly 25 rows are returned
async def fetch_trades_for_user_market_async(session: aiohttp.ClientSession,
                                             bucket: TokenBucket,
                                             user_addr: str,
                                             condition_id: str,
                                             page_size: int = 25,
                                             max_offset: int = 1000) -> list[dict]:
    out, offset = [], 0
    while True:
        params = {"user": user_addr, "market": condition_id, "limit": page_size, "offset": offset}
        batch = await _get_json(session, bucket, params)
        if not batch:
            break
        out.extend(batch)
        if len(batch) != page_size:
            break
        offset += page_size
        if offset > max_offset:
            break
    return out

async def get_trades_for_market_async(condition_id, clobs) -> List[dict]:
    takers = get_all_takers(clobs)
    
    bucket = TokenBucket(RPS, CAPACITY)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    results: List[dict] = []

    async with aiohttp.ClientSession(connector=connector) as session:
        async def worker(addr: str):
            async with sem:
                trades = await fetch_trades_for_user_market_async(session, bucket, addr, condition_id)
                if trades:
                    results.extend(trades)

        # pipeline with periodic progress prints
        tasks = []
        for i, a in enumerate(takers, 1):
            tasks.append(asyncio.create_task(worker(a)))
        await asyncio.gather(*tasks)

    return results

def get_trades_for_market(condition_id, clobs):
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

