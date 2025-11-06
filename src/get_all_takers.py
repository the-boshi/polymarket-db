import time
import requests
import random

SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn"
PAGE_SIZE = 1000  # typical safe limit
DEC_CASH = 6
DEC_SHARES = 6

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
# imports needed:
# import time, random, json, requests

def fetch_page(asset_id, last_id, query_string, page_size, max_retries=7):
    variables_base = {"asset": asset_id, "lastId": last_id}
    trial_sizes = [page_size, 500, 250, 100, 50]

    for size in trial_sizes:
        variables = dict(variables_base, pageSize=size)
        backoff = 0.5
        last_status = None
        last_body = None
        last_err = None

        for attempt in range(max_retries):
            try:
                # small jitter to avoid sync spikes
                time.sleep(random.random() * 0.05)

                r = requests.post(
                    SUBGRAPH_URL,
                    json={"query": query_string, "variables": variables},
                    headers={"Content-Type": "application/json"},
                    timeout=60,  # was 30
                )
                last_status = r.status_code

                # Retryable HTTP
                if r.status_code in (408, 429, 500, 502, 503, 504):
                    last_body = r.text[:200] if r.text else None

                    # honor Retry-After if present
                    ra = r.headers.get("Retry-After")
                    delay = float(ra) if ra and ra.replace('.', '', 1).isdigit() \
                           else backoff + random.random() * 0.5
                    time.sleep(delay)
                    backoff = min(backoff * 2, 8.0)

                    # if persistent 503, drop to smaller page size
                    if r.status_code == 503 and attempt >= 1:
                        break  # try next size
                    continue

                r.raise_for_status()

                # JSON / GraphQL handling
                try:
                    data = r.json()
                except ValueError as e:
                    last_err = f"json_decode: {e}"
                    last_body = r.text[:200] if r.text else None
                    time.sleep(backoff + random.random() * 0.5)
                    backoff = min(backoff * 2, 8.0)
                    continue

                if "errors" in data:
                    msg = data["errors"][0].get("message", "GraphQL error")
                    # transient messages → retry
                    if any(s in msg.lower() for s in ("timeout", "temporar", "overload", "rate")) and attempt < max_retries - 1:
                        last_err = msg
                        time.sleep(backoff + random.random() * 0.5)
                        backoff = min(backoff * 2, 8.0)
                        continue
                    raise RuntimeError(msg)

                return data["data"]["orderFilledEvents"]

            except (requests.Timeout, requests.ConnectionError) as e:
                last_err = str(e)
                time.sleep(backoff + random.random() * 0.5)
                backoff = min(backoff * 2, 8.0)
            except requests.HTTPError as e:
                last_err = str(e)
                # non-retryable HTTP here
                break

        # next smaller size
        continue

    raise RuntimeError(
        f"Failed to get users after retries status={last_status} "
        f"body[:200]={last_body!r} err={last_err!r} "
        f"vars={{'asset': {asset_id}, 'lastId': {last_id}, 'pageSize': {trial_sizes[-1]}}}"
    )

def get_all_takers(assets):
    unique_takers = []
    all_pages = []
    for asset_id in assets:
        last_id = ""
        total_buy = 0
        total_sell = 0

        while True:
            page = fetch_page(asset_id, last_id, MAKER_QUERY, PAGE_SIZE)
            if not page:
                break
            all_pages.extend(page)
            total_sell += len(page)
            last_id = page[-1]["id"]

        last_id = ""
        while True:
            page = fetch_page(asset_id, last_id, TAKER_QUERY, PAGE_SIZE)
            if not page:
                break
            all_pages.extend(page)
            total_buy += len(page)
            last_id = page[-1]["id"]
            
    takers = list({d['taker'] for d in all_pages})
    unique_takers = list(set(takers))
    return unique_takers

