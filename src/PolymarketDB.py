# polymarket_db.py
import sqlite3
from pathlib import Path
from typing import Optional

from utils import _market_trades_path, ensure_list, infer_winner

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  slug     TEXT UNIQUE NOT NULL,
  title    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
  condition_id TEXT PRIMARY KEY,
  event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
  slug         TEXT UNIQUE NOT NULL,
  question     TEXT NOT NULL,
  start_ts     TEXT NOT NULL,       -- when market opens (ISO8601 UTC)
  end_ts       TEXT,       -- when market closes/resolves
  closed       INTEGER NOT NULL,    -- 0/1
  outcome      TEXT NOT NULL,       -- 'yes', 'no', 'bulls', etc.
  trades_path  TEXT NOT NULL,       -- path to the market's Parquet file
  trades_rows  INTEGER,             -- number of trades
  volume       REAL,                -- total traded volume
  CONSTRAINT markets_closed_chk CHECK (closed IN (0,1))
);

CREATE TABLE IF NOT EXISTS assets (
  asset_id     TEXT PRIMARY KEY,
  condition_id TEXT NOT NULL REFERENCES markets(condition_id) ON DELETE CASCADE,
  outcome      TEXT NOT NULL,
  UNIQUE (condition_id, outcome)
);

CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(event_id);
"""

UPSERT_EVENT = """
INSERT INTO events (event_id, slug, title)
VALUES (?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET
  slug = excluded.slug,
  title = excluded.title;
"""

UPSERT_MARKET = """
INSERT INTO markets (
  condition_id, event_id, slug, question,
  start_ts, end_ts, closed, outcome,
  trades_path, trades_rows, volume
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(condition_id) DO UPDATE SET
  event_id    = excluded.event_id,
  slug        = excluded.slug,
  question    = excluded.question,
  start_ts    = excluded.start_ts,
  end_ts      = excluded.end_ts,
  closed      = excluded.closed,
  outcome     = excluded.outcome,
  trades_path = excluded.trades_path,
  trades_rows = excluded.trades_rows,
  volume      = excluded.volume;
"""

UPSERT_ASSET = """
INSERT INTO assets (asset_id, condition_id, outcome)
VALUES (?, ?, ?)
ON CONFLICT(asset_id) DO UPDATE SET
  condition_id = excluded.condition_id,
  outcome      = excluded.outcome;
"""

class PolymarketDB:
    """
    Minimal metadata store for closed markets:
      - SQLite file at: data_root / 'markets.sqlite'
      - Trades are stored separately as Parquet files; we only keep their path.

    Methods:
      - __init__(data_root)
      - close()
      - upsert_event(event_id, slug, title)
      - upsert_market(condition_id, event_id, slug, question, start_ts, end_ts, closed, outcome, volume, trades_path)
      - upsert_asset(asset_id, condition_id, outcome)
    """

    def __init__(self, data_root: str | Path):
        self.data_root = Path(data_root)
        self.data_root.mkdir(parents=True, exist_ok=True)
        self.sqlite_path = self.data_root / "markets.sqlite"
        self.conn = sqlite3.connect(self.sqlite_path.as_posix(), check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys=ON;")

        with self.conn:
            self.conn.executescript(DDL)

    def close(self) -> None:
        if getattr(self, "conn", None):
            self.conn.close()
            self.conn = None

    # -------------------- Upserts --------------------

    def upsert_event(self, event_id: str, slug: str, title: str) -> None:
        with self.conn:
            self.conn.execute(UPSERT_EVENT, (event_id, slug, title))

    def upsert_market(
        self,
        condition_id: str,
        event_id: str,
        slug: str,
        question: str,
        start_ts: str,   # ISO8601 UTC
        end_ts: str,     # ISO8601 UTC
        closed: bool,
        outcome: str,    # 'yes' | 'no' | 'unknown'
        trades_path: str,
        trades_rows: Optional[int] = None,
        volume: Optional[float] = None,
    ) -> None:
        with self.conn:
            self.conn.execute(
                UPSERT_MARKET,
                (
                    condition_id, event_id, slug, question,
                    start_ts, end_ts, int(bool(closed)), outcome,
                    trades_path, trades_rows, volume
                ),
            )

    def upsert_asset(self, asset_id: str, condition_id: str, outcome: str) -> None:
        """
        Insert or update an asset (Yes/No token) for a given market.
        Each market has two rows: outcome='yes' and outcome='no'.

        Args:
            asset_id: unique token ID of the outcome asset (clobTokenId).
            condition_id: ID of the parent market (foreign key).
            outcome: 'yes', 'no', 'bulls', etc.
        """
        with self.conn:
            self.conn.execute(UPSERT_ASSET, (asset_id, condition_id, outcome))

    # -------------------- Ingest ---------------------

    def ingest_market_metadata(self, market: dict):
        # -------- parse core fields ----------
        condition_id = str(market["conditionId"])
        evt = (market.get("events") or [{}])[0]
        event_id   = str(evt.get("id", ""))
        event_slug = str(evt.get("slug", event_id or "unknown_event"))
        event_title = str(evt.get("title", event_slug))

        slug      = market.get("slug") or condition_id
        question  = market.get("question", "")

        start_ts  = market.get("startDate")
        end_ts    = market.get("endDate")
        
        closed    = bool(market.get("closed", False))

        volume = market.get("volume")
        volume = float(volume) if volume is not None else None

        # lists can be JSON strings; normalize
        outcomes = [str(x) for x in ensure_list(market.get("outcomes"))]       # e.g. ["Yes","No"]
        clobs    = [str(x) for x in ensure_list(market.get("clobTokenIds"))]   # aligned asset ids
        prices   = ensure_list(market.get("outcomePrices"))                     # e.g. ["1","0"]

        # infer winner
        winner_name, winner_id = infer_winner(outcomes, prices, clobs)
        outcome = (winner_name or "").strip().lower()

        trades_path = _market_trades_path(self.data_root, event_slug, slug)

        # -------- upsert EVENT ----------
        self.upsert_event(event_id=event_id, slug=event_slug, title=event_title)

        # -------- upsert MARKET ----------
        self.upsert_market(
            condition_id=condition_id,
            event_id=event_id,
            slug=slug,
            question=question,
            start_ts=start_ts,
            end_ts=end_ts,
            closed=closed,
            outcome=outcome,
            trades_path=str(trades_path),
            volume=volume
        )
            
        # -------- upsert ASSET ----------
        asset_outcomes = {}
        for outcome, asset_id in zip(outcomes, clobs):
            asset_id = str(asset_id)
            outcome = str(outcome).strip().lower()
            self.upsert_asset(asset_id=asset_id, outcome=outcome, condition_id=condition_id)
            asset_outcomes[asset_id] = outcome

        return [condition_id, slug, clobs, outcomes, trades_path]






