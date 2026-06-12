"""Indexer for Polymarket trades from the Polygon blockchain."""

import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from tqdm import tqdm

from src.common.indexer import Indexer
from src.indexers.polymarket.blockchain import (
    CTF_EXCHANGE,
    NEGRISK_CTF_EXCHANGE,
    POLYMARKET_START_BLOCK,
    PolygonClient,
)

DATA_DIR = Path("data/polymarket/trades")
CURSOR_FILE = Path("data/polymarket/.backfill_block_cursor")
DEFAULT_CHUNK_SIZE = int(os.getenv("TRADES_CHUNK_SIZE", "200"))
TRADES_MAX_WORKERS = int(os.getenv("TRADES_MAX_WORKERS", "6"))
TRADES_INFLIGHT_CHUNKS = int(os.getenv("TRADES_INFLIGHT_CHUNKS", "8"))
TRADES_CHUNK_RETRIES = int(os.getenv("TRADES_CHUNK_RETRIES", "8"))
TRADES_BATCH_PAUSE_SEC = float(os.getenv("TRADES_BATCH_PAUSE_SEC", "0.75"))


class PolymarketTradesIndexer(Indexer):
    """Fetches and stores Polymarket trades from the Polygon blockchain."""

    def __init__(
        self,
        from_block: Optional[int] = None,
        to_block: Optional[int] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        super().__init__(
            name="polymarket_trades",
            description="Backfills Polymarket trades from Polygon blockchain to parquet files",
        )
        self._from_block = from_block
        self._to_block = to_block
        self._chunk_size = chunk_size

    def run(self) -> None:
        """Backfill all Polymarket trades from the Polygon blockchain.

        This fetches OrderFilled events from both CTF Exchange contracts
        (regular and NegRisk) and saves them to parquet files.
        """
        BATCH_SIZE = 10000
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

        client = PolygonClient()
        current_block = client.get_block_number()

        # Determine starting block
        from_block = self._from_block
        if from_block is None:
            if CURSOR_FILE.exists():
                try:
                    from_block = int(CURSOR_FILE.read_text().strip())
                    print(f"Resuming from block {from_block}")
                except (ValueError, TypeError):
                    from_block = POLYMARKET_START_BLOCK
            else:
                from_block = POLYMARKET_START_BLOCK

        to_block = self._to_block
        if to_block is None:
            to_block = current_block

        print(f"Fetching trades from block {from_block} to {to_block}")
        print(f"Total blocks: {to_block - from_block:,}")

        all_trades = []
        total_saved = 0
        contracts = [
            ("CTF Exchange", CTF_EXCHANGE),
            ("NegRisk CTF Exchange", NEGRISK_CTF_EXCHANGE),
        ]

        def get_next_chunk_idx():
            existing = list(DATA_DIR.glob("trades_*.parquet"))
            if not existing:
                return 0
            indices = []
            for f in existing:
                parts = f.stem.split("_")
                if len(parts) >= 2:
                    try:
                        indices.append(int(parts[1]))
                    except ValueError:
                        pass
            return max(indices) + BATCH_SIZE if indices else 0

        def save_batch(trades_batch):
            nonlocal total_saved
            if not trades_batch:
                return
            blocks = [int(row.get("block_number", 0) or 0) for row in trades_batch]
            block_start = min(blocks)
            block_end = max(blocks)
            chunk_path = DATA_DIR / f"trades_{block_start}_{block_end}.parquet"
            if chunk_path.exists():
                chunk_idx = get_next_chunk_idx()
                chunk_path = DATA_DIR / f"trades_{chunk_idx}_{chunk_idx + BATCH_SIZE}.parquet"
            df = pd.DataFrame(trades_batch)
            df.to_parquet(chunk_path)
            total_saved += len(trades_batch)
            tqdm.write(f"Saved {len(trades_batch)} trades to {chunk_path.name}")

        # Build list of chunk ranges
        ranges = []
        current = from_block
        while current <= to_block:
            end = min(current + self._chunk_size - 1, to_block)
            ranges.append((current, end))
            current = end + 1

        # Process by block range, fetching from both contracts for each range
        total_chunks = len(ranges)
        pbar = tqdm(total=total_chunks, desc="Backfilling", unit=" chunks")

        def _fetch_chunk_once(chunk_start: int, chunk_end: int) -> list[dict]:
            fetched_at = datetime.utcnow()
            chunk_rows: list[dict] = []

            def fetch_contract(contract_name: str, contract_address: str) -> list[dict]:
                rows: list[dict] = []
                trades = client.get_trades(
                    from_block=chunk_start,
                    to_block=chunk_end,
                    contract_address=contract_address,
                )
                for trade in trades:
                    trade_dict = asdict(trade)
                    trade_dict["maker_asset_id"] = str(trade_dict["maker_asset_id"])
                    trade_dict["taker_asset_id"] = str(trade_dict["taker_asset_id"])
                    trade_dict["_fetched_at"] = fetched_at
                    trade_dict["_contract"] = contract_name
                    rows.append(trade_dict)
                return rows

            with ThreadPoolExecutor(max_workers=2) as contract_pool:
                futures = [
                    contract_pool.submit(fetch_contract, contract_name, contract_address)
                    for contract_name, contract_address in contracts
                ]
                for future in futures:
                    chunk_rows.extend(future.result())
            return chunk_rows

        def _is_transient_rpc_error(exc: Exception) -> bool:
            if PolygonClient._should_split_log_range(exc):
                return False
            err = str(exc).lower()
            return any(
                token in err
                for token in (
                    "429",
                    "too many requests",
                    "timeout",
                    "timed out",
                    "connection",
                    "reset",
                    "502",
                    "503",
                    "504",
                    "gateway",
                    "rate limit",
                )
            )

        def fetch_chunk_trades(chunk_start: int, chunk_end: int) -> list[dict]:
            last_err: Exception | None = None
            for attempt in range(TRADES_CHUNK_RETRIES):
                try:
                    return _fetch_chunk_once(chunk_start, chunk_end)
                except Exception as exc:
                    last_err = exc
                    if PolygonClient._should_split_log_range(exc) and chunk_end > chunk_start:
                        mid = (chunk_start + chunk_end) // 2
                        tqdm.write(
                            f"Chunk {chunk_start}-{chunk_end} too large; "
                            f"splitting at {mid}"
                        )
                        time.sleep(0.05)
                        return fetch_chunk_trades(chunk_start, mid) + fetch_chunk_trades(
                            mid + 1, chunk_end
                        )
                    if not _is_transient_rpc_error(exc):
                        raise
                    wait_s = min(120.0, 2.0 ** attempt)
                    tqdm.write(
                        f"Chunk {chunk_start}-{chunk_end} transient error ({exc}); "
                        f"retry {attempt + 1}/{TRADES_CHUNK_RETRIES} in {wait_s:.0f}s"
                    )
                    time.sleep(wait_s)
            raise last_err  # type: ignore[misc]

        def commit_chunk(chunk_end: int, chunk_trades: list[dict]) -> None:
            nonlocal all_trades
            all_trades.extend(chunk_trades)
            pbar.update(1)
            pbar.set_postfix(
                block=chunk_end,
                buffer=len(all_trades),
                saved=total_saved,
            )
            while len(all_trades) >= BATCH_SIZE:
                save_batch(all_trades[:BATCH_SIZE])
                all_trades = all_trades[BATCH_SIZE:]
            CURSOR_FILE.write_text(str(chunk_end))

        interrupted = False
        try:
            next_to_commit = 0
            pending: dict[int, tuple[int, list[dict]]] = {}
            with ThreadPoolExecutor(max_workers=TRADES_MAX_WORKERS) as chunk_pool:
                for batch_start in range(0, len(ranges), TRADES_INFLIGHT_CHUNKS):
                    if batch_start > 0 and TRADES_BATCH_PAUSE_SEC > 0:
                        time.sleep(TRADES_BATCH_PAUSE_SEC)
                    batch = ranges[batch_start : batch_start + TRADES_INFLIGHT_CHUNKS]
                    futures = {
                        chunk_pool.submit(fetch_chunk_trades, chunk_start, chunk_end): batch_start + local_idx
                        for local_idx, (chunk_start, chunk_end) in enumerate(batch)
                    }
                    for future in as_completed(futures):
                        idx = futures[future]
                        chunk_end = ranges[idx][1]
                        chunk_trades = future.result()
                        pending[idx] = (chunk_end, chunk_trades)
                        while next_to_commit in pending:
                            end_block, rows = pending.pop(next_to_commit)
                            commit_chunk(end_block, rows)
                            next_to_commit += 1

        except KeyboardInterrupt:
            interrupted = True
            print("\nInterrupted. Progress saved.")
        except Exception as exc:
            traceback.print_exc()
            print(f"\nBackfill failed at cursor block {CURSOR_FILE.read_text().strip() if CURSOR_FILE.exists() else from_block}: {exc}")
            raise SystemExit(1) from exc
        finally:
            pbar.close()

        # Save remaining trades
        if all_trades:
            save_batch(all_trades)

        # Only clean up cursor on successful completion
        if not interrupted and CURSOR_FILE.exists():
            CURSOR_FILE.unlink()

        print(f"\nBackfill complete: {total_saved} trades saved")
