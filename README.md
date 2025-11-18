# polymarket-db

**Tools and scripts for building a structured, queryable Polymarket dataset from raw trade and market APIs.**

## Overview

This repository provides a workflow to download Polymarket market metadata, user info, and trade history, and build a local database (SQLite) for analysis and model training.  

## Usage

```bash
# 1) Download markets metadata into ./markets/
# --resume skips over dates already fetched
python fetch_markets.py \
  --start 2024-10-01 \ 
  --end   2024-10-31 \
  --resume \

# 2) Dowload user wallet ID's for markets in ./markets/, ingest metadata and ID's into polymarket.db
# --resumes skips over users already ingested
python fetch_users.py \
  --start 2024-10-01 \
  --end   2024-10-31 \
  --resume \

# 3) For each user in the database, fetch all trades history. Ingests to db and also stores in sharded folder ./raw_trades/
# --resumes skips over users already ingested
python fetch_trades.py \
  --only \
  --resume \
```

## Requirements

- Python 3.10+
- Packages: `requests`, `pandas`, `aiohttp`, `nest-asyncio`, `ipykernel`, `pyarrow`.  
  Install:
  ```bash
  pip install -r requirements.txt
  ```

## Notes

- Re-running with `--resume` is idempotent.
- Sometimes the APIs timeout on long requests, often re-running with `--resume` fixes issue.
- Fetching the whole market can take days to complete due to tight rate limiters. 
- Logs for each section will be created at ./logs.
