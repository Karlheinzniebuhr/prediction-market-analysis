"""Indexer for Polymarket markets data."""

import re
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.indexer import Indexer
from src.indexers.polymarket.client import PolymarketClient

DATA_DIR = Path("data/polymarket/markets")
CURSOR_FILE = Path("data/polymarket/.backfill_markets_cursor")
CHUNK_SIZE = 10000


class PolymarketMarketsIndexer(Indexer):
    """Fetches and stores Polymarket markets data."""

    def __init__(self):
        super().__init__(
            name="polymarket_markets",
            description="Backfills Polymarket markets data to parquet files using keyset pagination",
        )

    def run(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

        client = PolymarketClient()

        after_cursor = None
        if CURSOR_FILE.exists():
            try:
                after_cursor = CURSOR_FILE.read_text().strip()
                if after_cursor:
                    print(f"Resuming from cursor: {after_cursor}")
                else:
                    after_cursor = None
            except Exception:
                after_cursor = None

        all_markets = []
        total = 0

        # If resuming, we might want to know how many we have already fetched.
        # To avoid overwriting existing files when resuming, let's find the max index
        # in the existing markets_*.parquet files and start total from there.
        existing_files = list(DATA_DIR.glob("markets_*.parquet"))
        if existing_files:
            pattern = re.compile(r"markets_(\d+)_(\d+)\.parquet")
            max_end = 0
            for f in existing_files:
                m = pattern.match(f.name)
                if m:
                    max_end = max(max_end, int(m.group(2)))
            if after_cursor is not None:
                total = max_end
                print(f"Found existing files up to index {max_end}. Starting total at {total}.")
            else:
                # If starting fresh, let's delete existing markets files to avoid duplicates/overlaps
                print("Starting fresh. Deleting existing markets parquet files...")
                for f in existing_files:
                    try:
                        f.unlink()
                    except Exception as e:
                        print(f"Error deleting {f}: {e}")

        while True:
            try:
                markets, next_cursor = client.get_markets_keyset(limit=100, after_cursor=after_cursor)
            except Exception as e:
                print(f"Error fetching markets: {e}")
                break

            if not markets:
                print("No more markets returned.")
                break

            fetched_at = datetime.utcnow()
            for market in markets:
                record = asdict(market)
                record["_fetched_at"] = fetched_at
                all_markets.append(record)

            total += len(markets)
            print(f"Fetched {len(markets)} markets (total: {total})")

            # Save in chunks
            while len(all_markets) >= CHUNK_SIZE:
                chunk = all_markets[:CHUNK_SIZE]
                chunk_start = total - len(all_markets)
                chunk_path = DATA_DIR / f"markets_{chunk_start}_{chunk_start + CHUNK_SIZE}.parquet"
                pd.DataFrame(chunk).to_parquet(chunk_path)
                all_markets = all_markets[CHUNK_SIZE:]

            if next_cursor:
                after_cursor = next_cursor
                CURSOR_FILE.write_text(after_cursor)
            else:
                break

        # Save remaining markets
        if all_markets:
            chunk_start = total - len(all_markets)
            chunk_path = DATA_DIR / f"markets_{chunk_start}_{chunk_start + len(all_markets)}.parquet"
            pd.DataFrame(all_markets).to_parquet(chunk_path)

        if CURSOR_FILE.exists():
            CURSOR_FILE.unlink()

        client.close()
        print(f"\nBackfill complete: {total} markets fetched")
