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

