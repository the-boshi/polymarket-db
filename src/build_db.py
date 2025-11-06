#!/usr/bin/env python3
import argparse

from ingest_markets import ingest_all_markets_folder

def main():
    """
    Main entry point: iterate over all .jsonl/.jsol files in a market-data folder
    and ingest them into the local SQLite DB.
    """
    parser = argparse.ArgumentParser(description="Polymarket market ingestion utility")
    parser.add_argument(
        "--root-data",
        default="data",
        help="Root directory containing the SQLite DB and trades_by_event/ folder (default: data)"
    )
    parser.add_argument(
        "--market-data",
        default="markets",
        help="Folder containing market JSONL/JSOL files (default: markets)"
    )
    parser.add_argument(
        "--glob-pattern",
        default="*.jsonl",
        help="Glob pattern for files to ingest (default: *.jsonl)"
    )

    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip markets whose Parquet already exists with rows"
    )

    args = parser.parse_args()

    print("=== Polymarket Ingestion ===")
    print(f"Data folder: {args.root_data}")
    print(f"Markets folder: {args.market_data}")
    print(f"Pattern: {args.glob_pattern}")
    print(f"Skip existing: {args.skip_existing}")

    ingest_all_markets_folder(
        root_data=args.root_data,
        market_data=args.market_data,
        glob_pattern=args.glob_pattern,
        skip_existing=args.skip_existing,
    )

    print("=== Ingestion complete ===")


if __name__ == "__main__":
    main()

