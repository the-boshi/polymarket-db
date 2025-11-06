# ingest_markets.py
# Ingest markets JSONL, upsert to DB, fetch trades, and log to <data_root>/logs/ingest.log

import time
import json
import sqlite3
import traceback
from pathlib import Path
from typing import List, Dict, Any

from PolymarketDB import PolymarketDB
from get_market_trades import get_trades_save_parquet
from utils import parquet_num_rows


# ---------- logging ----------

import logging
from pathlib import Path
from datetime import datetime, timezone

def setup_logger(data_root: str | Path) -> logging.Logger:
    """
    Create an 'ingest' logger.
    Each script run gets its own log file:
        <data_root>/logs/ingest-YYYYMMDD-HHMMSS-xxxxxx.log
    Idempotent inside one run.
    """
    data_root = Path(data_root)
    log_dir = data_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Unique log file per run
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_file = log_dir / f"ingest-{ts}.log"

    logger = logging.getLogger(f"ingest-{ts}")
    logger.setLevel(logging.INFO)

    # Only add handlers if logger is new
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        # Optional: remove if file-only
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

        logger.info(f"Logging to {log_file}")

    return logger


# ---------- helpers ----------

def _to_bool(x) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "yes", "y", "t"}
    return bool(x)


# ---------- core ingestion ----------

def ingest_markets_jsonl(db: PolymarketDB, jsonl_path: str, skip_existing: bool, logger: logging.Logger):
    """
    Ingest all markets from a JSONL file.
    Robust against per-line errors. Then fetch trades for successful rows.
    """
    logger.info(f"[upsert] Upserting markets to {db.sqlite_path}")

    fail_lines: List[Dict[str, Any]] = []
    success_lines: List[list] = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
                condition_id, slug, clobs, outcomes, trades_path = db.ingest_market_metadata(m)
                success_lines.append([i, line, condition_id, slug, clobs, outcomes, trades_path])

            except json.JSONDecodeError as e:
                logger.error(f"line {i}: invalid JSON: {e}")
                fail_lines.append({
                    "line_no": i, "err_type": "JSONDecodeError",
                    "msg": str(e), "line": line[:200]
                })

            except sqlite3.IntegrityError as e:
                cid = None
                try:
                    if isinstance(m, dict):
                        cid = m.get("conditionId")
                except Exception:
                    pass
                logger.error(f"line {i}: IntegrityError: {e} (conditionId={cid})")
                fail_lines.append({
                    "line_no": i, "err_type": "IntegrityError",
                    "msg": str(e), "conditionId": cid, "line": line[:200]
                })

            except Exception as e:
                logger.error(f"line {i}: {type(e).__name__}: {e}")
                tb = "".join(traceback.format_exception_only(type(e), e)).strip()
                fail_lines.append({
                    "line_no": i, "err_type": type(e).__name__,
                    "msg": str(e), "tb": tb, "line": line[:200]
                })

    total = len(success_lines) + len(fail_lines)
    logger.info(f"[upsert] {len(success_lines)}/{total} markets upserted, {len(fail_lines)}/{total} failed")

    # Optionally persist failures for debugging
    if fail_lines:
        fail_path = Path(db.data_root) / "logs" / (Path(jsonl_path).stem + "-ingest_failures.jsonl")
        with open(fail_path, "w", encoding="utf-8") as fh:
            for rec in fail_lines:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logger.info(f"[upsert] wrote failure details to {fail_path}")

    # Fetch trades for successes
    logger.info("[fetch] Fetching all trades")
    fetch_sucesses = fetch_fails = fetch_skips = 0

    for i, line, condition_id, slug, clobs, outcomes, trades_path in success_lines:
        try:
            logger.info(f"[fetch] {slug}")
            did_write = get_trades_save_parquet(condition_id, clobs, trades_path, skip_existing)
            if did_write:
                fetch_sucesses += 1
                num_rows = parquet_num_rows(trades_path)
                logger.info(f"[fetch] wrote {num_rows} rows to {trades_path}")
            else:
                fetch_skips += 1
                logger.info("[fetch] skipped")

        except json.JSONDecodeError as e:
            fetch_fails += 1
            logger.error(f"line {i}: invalid JSON ({e})")

        except Exception as e:
            fetch_fails += 1
            logger.error(f"line {i}: {type(e).__name__}: {e}")

    fetch_total = fetch_sucesses + fetch_skips + fetch_fails
    logger.info(f"[fetch] {fetch_sucesses}/{fetch_total} markets fetched, "
                f"{fetch_skips}/{fetch_total} skipped, {fetch_fails}/{fetch_total} failed")


def ingest_all_markets_folder(
    root_data: str = "data",
    market_data: str = "markets",
    glob_pattern: str = "*.jsonl",
    skip_existing: bool | str = "False"
):
    """
    Walk market_data, find *.jsonl, and ingest each file.
    Logs to <root_data>/logs/ingest.log.
    """
    logger = setup_logger(root_data)
    skip_existing = _to_bool(skip_existing)

    folder_path = Path(market_data)
    logger.info(f"[ingest] Starting ingestion from {folder_path.resolve()} with glob \"{glob_pattern}\"")

    files = sorted(folder_path.glob(glob_pattern))
    if not files:
        logger.info(f"[ingest] No files in {folder_path.resolve()} that match {glob_pattern}")
        return

    logger.info(f"[ingest] Found {len(files)} file(s) under {folder_path.resolve()} with glob \"{glob_pattern}\"")

    t0 = time.time()
    for i, fp in enumerate(files, 1):
        logger.info(f"========== FILE {i}/{len(files)}: {fp.name} ==========")
        f_start = time.time()
        db = None
        try:
            db = PolymarketDB(root_data)
            ingest_markets_jsonl(
                db=db,
                jsonl_path=str(fp),
                skip_existing=skip_existing,
                logger=logger
            )
        except Exception as e:
            logger.error(f"[ingest] ERROR processing {fp.name}: {e}")
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
            dt = time.time() - f_start
            logger.info(f"[ingest] Finished {fp.name} in {dt:.1f}s")

    dt_all = time.time() - t0
    logger.info(f"[ingest] ALL DONE in {dt_all/60:.1f} min")

