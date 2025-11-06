from datetime import datetime, timezone
from pathlib import Path
import json

def _market_trades_path(data_root: Path, event_slug: str, market_slug: str) -> Path:
    event_dir = data_root / "trades_by_event" / f"{event_slug}"
    event_dir.mkdir(parents=True, exist_ok=True)
    return event_dir / f"{market_slug}.parquet"

def ensure_list(x):
    if x is None: return []
    if isinstance(x, list): return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, list) else [x]
        except Exception:
            return [x]
    return [x]

def infer_winner(outcomes_raw, prices_raw, clob_ids_raw):
    """
    Returns (winner_name, winner_asset_id) or (None, None)
    Uses argmax over outcomePrices; works with strings or lists.
    """
    outcomes = ensure_list(outcomes_raw)
    clobs    = ensure_list(clob_ids_raw)
    prices_l = ensure_list(prices_raw)
    try:
        prices = [float(p) for p in prices_l]
    except Exception:
        return None, None

    if not (outcomes and clobs and prices): return None, None
    if not (len(outcomes) == len(clobs) == len(prices)): return None, None

    k = max(range(len(prices)), key=lambda i: prices[i])
    return str(outcomes[k]), str(clobs[k])

def parquet_num_rows(path: str | Path) -> int:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return 0
    try:
        import pyarrow.parquet as pq
        return int((pq.ParquetFile(str(p)).metadata.num_rows) or 0)
    except Exception:
        return 0  # treat as empty if metadata read fails


