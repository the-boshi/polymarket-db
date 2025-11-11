# remove: import asyncio, aiohttp
import time, random, logging
import requests
from typing import List, Set, Optional

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

def fetch_page(
    session: requests.Session,
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
                time.sleep(random.random() * 0.05)  # jitter
                resp = session.post(
                    SUBGRAPH_URL,
                    json={"query": query_string, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                last_status = resp.status_code

                if resp.status_code in RETRY_STATUS:
                    last_body = resp.text[:200]
                    ra = resp.headers.get("Retry-After")
                    try:
                        delay = float(ra) if ra else backoff + random.random() * 0.5
                    except ValueError:
                        delay = backoff + random.random() * 0.5
                    time.sleep(delay)
                    backoff = min(backoff * 2, 8.0)
                    if resp.status_code == 503 and attempt >= 1:
                        break  # drop to smaller page
                    continue

                if resp.status_code >= 400:
                    last_body = resp.text[:200]
                    last_err = f"HTTP {resp.status_code}: {last_body!r}"
                    break

                try:
                    data = resp.json()
                except Exception as e:
                    last_err = f"json_decode: {e}"
                    last_body = resp.text[:200]
                    time.sleep(backoff + random.random() * 0.5)
                    backoff = min(backoff * 2, 8.0)
                    continue

                if "errors" in data:
                    msg = data["errors"][0].get("message", "GraphQL error")
                    if any(s in msg.lower() for s in ("timeout", "temporar", "overload", "rate")) and attempt < max_retries - 1:
                        last_err = msg
                        time.sleep(backoff + random.random() * 0.5)
                        backoff = min(backoff * 2, 8.0)
                        continue
                    raise RuntimeError(msg)

                return data["data"]["orderFilledEvents"]

            except requests.RequestException as e:
                last_err = str(e)
                time.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 8.0)

    raise RuntimeError(
        f"Failed after retries status={last_status} body[:200]={last_body!r} err={last_err!r} "
        f"vars={{'asset': {asset_id}, 'lastId': {last_id}, 'pageSize': {trial_sizes[-1]}}}"
    )

def drain_side_for_asset(
    session: requests.Session,
    asset_id: str,
    query: str,
) -> List[dict]:
    out: List[dict] = []
    last_id = ""
    while True:
        page = fetch_page(session, asset_id, last_id, query)
        if not page:
            break
        out.extend(page)
        last_id = page[-1]["id"]
        time.sleep(0.02)  # small polite delay
    return out


def process_asset(
    session: requests.Session,
    asset_id: str,
) -> List[dict]:
    maker_pages = drain_side_for_asset(session, asset_id, MAKER_QUERY)
    taker_pages = drain_side_for_asset(session, asset_id, TAKER_QUERY)
    return maker_pages + taker_pages


def get_all_takers(assets: List[str]) -> List[str]:
    all_pages: List[dict] = []
    with requests.Session() as session:
        for a in assets:
            pages = process_asset(session, a)
            all_pages.extend(pages)
    takers: Set[str] = {d["taker"] for d in all_pages if "taker" in d}
    return sorted(takers)
