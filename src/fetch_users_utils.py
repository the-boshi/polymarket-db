import asyncio, random, logging
from typing import List, Set, Optional
import aiohttp

SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"

MAKER_QUERY = """
query($asset: String!, $lastId: String!, $pageSize: Int!) {
  orderFilledEvents(
    where: { makerAssetId: $asset, id_gt: $lastId }
    orderBy: id
    orderDirection: asc
    first: $pageSize
  ) {
    id
    taker
  }
}
"""

TAKER_QUERY = """
query($asset: String!, $lastId: String!, $pageSize: Int!) {
  orderFilledEvents(
    where: { takerAssetId: $asset, id_gt: $lastId }
    orderBy: id
    orderDirection: asc
    first: $pageSize
  ) {
    id
    taker
  }
}
"""

RETRY_STATUS = {408, 429, 500, 502, 503, 504}

async def fetch_page_async(
    session: aiohttp.ClientSession,
    asset_id: str,
    last_id: str,
    query_string: str,
    *,
    max_retries: int = 7,
) -> Optional[List[dict]]:
    variables_base = {"asset": asset_id, "lastId": last_id}
    trial_sizes = [1000, 500, 250, 100, 50]

    for size in trial_sizes:
        variables = dict(variables_base, pageSize=size)
        backoff = 0.5
        last_status = None
        last_body = None
        last_err = None
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(random.random() * 0.05)  # jitter

                async with session.post(
                    SUBGRAPH_URL,
                    json={"query": query_string, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    last_status = resp.status

                    # retryable HTTP
                    if resp.status in RETRY_STATUS:
                        last_body = (await resp.text())[:200]
                        ra = resp.headers.get("Retry-After")
                        try:
                            delay = float(ra) if ra else backoff + random.random() * 0.5
                        except ValueError:
                            delay = backoff + random.random() * 0.5

                        await asyncio.sleep(delay)
                        backoff = min(backoff * 2, 8.0)
                        if resp.status == 503 and attempt >= 1:
                            break  # drop to smaller page
                        continue

                    # non-retryable errors
                    if resp.status >= 400:
                        last_body = (await resp.text())[:200]
                        last_err = f"HTTP {resp.status}: {last_body!r}"
                        break

                    # parse JSON
                    try:
                        data = await resp.json()
                    except Exception as e:
                        last_err = f"json_decode: {e}"
                        last_body = (await resp.text())[:200]
                        await asyncio.sleep(backoff + random.random() * 0.5)
                        backoff = min(backoff * 2, 8.0)
                        continue

                    if "errors" in data:
                        msg = data["errors"][0].get("message", "GraphQL error")
                        if any(s in msg.lower() for s in ("timeout", "temporar", "overload", "rate")) and attempt < max_retries - 1:
                            last_err = msg
                            await asyncio.sleep(backoff + random.random() * 0.5)
                            backoff = min(backoff * 2, 8.0)
                            continue
                        raise RuntimeError(msg)

                    return data["data"]["orderFilledEvents"]

            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                last_err = str(e)
                await asyncio.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 8.0)

        # try next smaller size
        continue

    raise RuntimeError(
        f"Failed after retries status={last_status} body[:200]={last_body!r} err={last_err!r} "
        f"vars={{'asset': {asset_id}, 'lastId': {last_id}, 'pageSize': {trial_sizes[-1]}}}"
    )

async def drain_side_for_asset(
    session: aiohttp.ClientSession,
    asset_id: str,
    query: str,
) -> List[dict]:
    out: List[dict] = []
    last_id = ""
    while True:
        page = await fetch_page_async(session, asset_id, last_id, query)
        if not page:
            break
        out.extend(page)
        last_id = page[-1]["id"]
        # small polite delay to reduce burstiness
        await asyncio.sleep(0.02)
    return out

async def process_asset(
    session: aiohttp.ClientSession,
    asset_id: str,
    maker_query: str,
    taker_query: str,
) -> List[dict]:
    # maker and taker can run concurrently for the asset
    maker_task = asyncio.create_task(drain_side_for_asset(session, asset_id, maker_query))
    taker_task = asyncio.create_task(drain_side_for_asset(session, asset_id, taker_query))
    maker_pages, taker_pages = await asyncio.gather(maker_task, taker_task)
    return maker_pages + taker_pages

async def get_all_takers_async(
    assets: List[str],
    maker_query: str,
    taker_query: str,
    concurrency: int = 8,
) -> List[str]:

    sem = asyncio.Semaphore(concurrency)

    async def _guarded(asset_id: str) -> List[dict]:
        async with sem:
            return await process_asset(session, asset_id, maker_query, taker_query)

    connector = aiohttp.TCPConnector(limit=concurrency * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        all_pages: List[dict] = []
        tasks = [asyncio.create_task(_guarded(a)) for a in assets]
        for coro in asyncio.as_completed(tasks):
            pages = await coro
            all_pages.extend(pages)


    takers: Set[str] = {d["taker"] for d in all_pages if "taker" in d}
    return sorted(takers)

# ---- usage example (sync wrapper) ----
def get_all_takers(assets: List[str]) -> List[str]:
    return asyncio.run(get_all_takers_async(assets, MAKER_QUERY, TAKER_QUERY, concurrency=8))