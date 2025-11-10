from datetime import datetime, date
from typing import Optional
import calendar
import json

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
    


def _month_bounds(month: str) -> tuple[date, date]:
    y, m = map(int, month.split("-"))
    last = calendar.monthrange(y, m)[1]
    return date(y, m, 1), date(y, m, last)

def _parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _market_start_date(m: dict) -> Optional[date]:
    s = m.get("startDate")
    if not s or len(s) < 10:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None

def market_in_range_by_start(m: dict, month: Optional[str], from_date: Optional[str], to_date: Optional[str]) -> bool:
    if not month and not from_date and not to_date:
        return True
    d = _market_start_date(m)
    if d is None:
        return False
    if month:
        lo, hi = _month_bounds(month)
    else:
        if from_date and to_date:
            a, b = _parse_ymd(from_date), _parse_ymd(to_date)
            lo, hi = (a, b) if a <= b else (b, a)
        elif from_date:
            lo = hi = _parse_ymd(from_date)
        elif to_date:
            lo = hi = _parse_ymd(to_date)
        else:
            return True
    return lo <= d <= hi



