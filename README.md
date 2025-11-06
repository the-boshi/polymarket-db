# polymarket-db

**Tools and scripts for building a structured, queryable Polymarket dataset from raw trade and market APIs.**

## Overview

This repository provides a two-step workflow to download Polymarket market metadata and build local databases (SQLite and Parquet) for analysis and model training.

## Quick start

```bash
# 1) Download markets into ./markets
python pull_markets.py \
  --start 2024-10-01 \
  --end   2024-10-31 \
  --limit 500 \
  --min_volume 1000 \
  --closed true \
  --outdir ./markets \
  --timeout 30

# 2) Build SQLite and Parquet from markets
python build_db.py \
  --root-data "./data" \
  --market-data "./markets" \
  --glob-pattern "*.jsonl" \
  --skip-existing
```

## Step 1 — Download markets metadata

Writes one JSONL per day into `./markets`.

**Script:** `pull_markets.py`  
**Args:**
- `--start YYYY-MM-DD`
- `--end YYYY-MM-DD`
- `--limit INT` per API page
- `--min_volume INT` filter
- `--closed {true,false}` include only closed or open
- `--outdir PATH` output folder (default `./markets`)
- `--timeout SECONDS` HTTP timeout

**Example:**
```bash
python pull_markets.py \
  --start 2024-09-01 --end 2024-11-01 \
  --limit 1000 --min_volume 0 --closed false \
  --outdir ./markets --timeout 20
```

Result layout:
```
./markets/
  2024-10-01.jsonl
  2024-10-02.jsonl
  ...
```

## Step 2 — Build the databases

Reads JSONL files from `./markets` and produces:
- SQLite at `./data/polymarket.db`
- Parquet datasets under `./data/parquet/...`

**Script:** `build_db.py`  
**Args:**
- `--root-data PATH` base folder for outputs (default `./data`)
- `--market-data PATH` folder with JSONL inputs (default `./markets`)
- `--glob-pattern PATTERN` file pattern within `--market-data`
- `--skip-existing` skip markets with existing data

**Examples:**

Single file:
```bash
python build_db.py \
  --root-data "./data" \
  --market-data "./markets" \
  --glob-pattern "2024-10-01.jsonl" \
  --skip-existing
```

Month range with shell glob:
```bash
python build_db.py \
  --root-data "./data" \
  --market-data "./markets" \
  --glob-pattern "2024-10-*.jsonl" \
  --skip-existing
```

All files:
```bash
python build_db.py \
  --root-data "./data" \
  --market-data "./markets" \
  --glob-pattern "*.jsonl" \
  --skip-existing
```

## Outputs

- `./data/markets.db`  
  Tables for events, markets, and assets metadata.

- `./data/trades_by_event/`  
  Columnar files partitioned by Polymarket markets, in directories named after Polymarket events. 

## Requirements

- Python 3.10+
- Packages: `requests`, `pandas`, `aiohttp`, `nest-asyncio`, `ipykernel`, `pyarrow`, `fastparquet`.  
  Install:
  ```bash
  pip install -r requirements.txt
  ```

## Notes

- Re-running with `--skip-existing` is idempotent.
- Sometimes the APIs timeout on long requests, often re-running with `--skip-existing` fixes issue.
