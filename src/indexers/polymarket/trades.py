"""Indexer for Polymarket trades from the Polygon blockchain."""

import os
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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
DEFAULT_CHUNK_SIZE = int(os.getenv("TRADES_CHUNK_SIZE", "100"))
TRADES_MAX_WORKERS = int(os.getenv("TRADES_MAX_WORKERS", "20"))
TRADES_INFLIGHT_CHUNKS = int(os.getenv("TRADES_INFLIGHT_CHUNKS", str(TRADES_MAX_WORKERS)))
TRADES_CHUNK_RETRIES = int(os.getenv("TRADES_CHUNK_RETRIES", "8"))
TRADES_BATCH_PAUSE_SEC = float(os.getenv("TRADES_BATCH_PAUSE_SEC", "0"))


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
        """Backfill all Polymarket trades from the Polygon blockchain."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)

        client = PolygonClient()
        current_block = client.get_block_number()

        from_block = self._from_block
        if from_block is None:
            if CURSOR_FILE.exists():
                try:
                    cursor_block = int(CURSOR_FILE.read_text().strip())
                    from_block = cursor_block + 1
                    print(f"Resuming from block {from_block} (after cursor {cursor_block})")
                except (ValueError, TypeError):
                    from_block = POLYMARKET_START_BLOCK
            else:
                from_block = POLYMARKET_START_BLOCK

        to_block = self._to_block or current_block

        print(f"Fetching trades from block {from_block} to {to_block}")
        print(f"Total blocks: {to_block - from_block:,}")
        print(
            "Throttle: "
            f"chunk={self._chunk_size}, workers={TRADES_MAX_WORKERS}, "
            f"inflight={TRADES_INFLIGHT_CHUNKS}, pause={TRADES_BATCH_PAUSE_SEC}s"
        )

        total_saved = 0
        contracts = [
            ("CTF Exchange", CTF_EXCHANGE),
            ("NegRisk CTF Exchange", NEGRISK_CTF_EXCHANGE),
        ]

        def save_chunk(chunk_start: int, chunk_end: int, trades_batch: list[dict]) -> int:
            nonlocal total_saved
            if not trades_batch:
                return 0
            chunk_path = DATA_DIR / f"trades_{chunk_start}_{chunk_end}.parquet"
            df = pd.DataFrame(trades_batch)
            df.to_parquet(chunk_path)
            total_saved += len(trades_batch)
            tqdm.write(f"Saved {len(trades_batch)} trades to {chunk_path.name}")
            return len(trades_batch)

        ranges: list[tuple[int, int]] = []
        current = from_block
        while current <= to_block:
            end = min(current + self._chunk_size - 1, to_block)
            ranges.append((current, end))
            current = end + 1

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
                    "400",
                    "bad request",
                    "too many requests",
                    "timeout",
                    "timed out",
                    "connection",
                    "reset",
                    "prematurely",
                    "chunkedencoding",
                    "protocolerror",
                    "broken pipe",
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
                        tqdm.write(f"Chunk {chunk_start}-{chunk_end} too large; splitting at {mid}")
                        time.sleep(0.05)
                        return fetch_chunk_trades(chunk_start, mid) + fetch_chunk_trades(mid + 1, chunk_end)
                    if not _is_transient_rpc_error(exc):
                        raise
                    wait_s = min(120.0, 2.0**attempt)
                    tqdm.write(
                        f"Chunk {chunk_start}-{chunk_end} transient error ({exc}); "
                        f"retry {attempt + 1}/{TRADES_CHUNK_RETRIES} in {wait_s:.0f}s"
                    )
                    time.sleep(wait_s)
            raise last_err  # type: ignore[misc]

        def commit_chunk(idx: int, chunk_trades: list[dict]) -> None:
            chunk_start, chunk_end = ranges[idx]
            saved = save_chunk(chunk_start, chunk_end, chunk_trades)
            pbar.update(1)
            pbar.set_postfix(block=chunk_end, saved=total_saved, last=saved)
            CURSOR_FILE.write_text(str(chunk_end))

        interrupted = False
        try:
            next_to_commit = 0
            pending: dict[int, list[dict]] = {}
            submit_idx = 0
            in_flight: dict = {}

            with ThreadPoolExecutor(max_workers=TRADES_MAX_WORKERS) as chunk_pool:
                while submit_idx < total_chunks and len(in_flight) < TRADES_INFLIGHT_CHUNKS:
                    chunk_start, chunk_end = ranges[submit_idx]
                    future = chunk_pool.submit(fetch_chunk_trades, chunk_start, chunk_end)
                    in_flight[future] = submit_idx
                    submit_idx += 1

                while in_flight:
                    done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
                    for future in done:
                        idx = in_flight.pop(future)
                        pending[idx] = future.result()

                        while next_to_commit in pending:
                            commit_chunk(next_to_commit, pending.pop(next_to_commit))
                            next_to_commit += 1
                            if TRADES_BATCH_PAUSE_SEC > 0:
                                time.sleep(TRADES_BATCH_PAUSE_SEC)

                        while submit_idx < total_chunks and len(in_flight) < TRADES_INFLIGHT_CHUNKS:
                            chunk_start, chunk_end = ranges[submit_idx]
                            new_future = chunk_pool.submit(fetch_chunk_trades, chunk_start, chunk_end)
                            in_flight[new_future] = submit_idx
                            submit_idx += 1

        except KeyboardInterrupt:
            interrupted = True
            print("\nInterrupted. Progress saved.")
        except Exception as exc:
            traceback.print_exc()
            cursor = CURSOR_FILE.read_text().strip() if CURSOR_FILE.exists() else str(from_block)
            print(f"\nBackfill failed at cursor block {cursor}: {exc}")
            raise SystemExit(1) from exc
        finally:
            pbar.close()

        if not interrupted and CURSOR_FILE.exists():
            CURSOR_FILE.unlink()

        print(f"\nBackfill complete: {total_saved} trades saved")
