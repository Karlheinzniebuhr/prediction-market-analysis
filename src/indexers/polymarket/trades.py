"""Indexer for Polymarket trades from the Polygon blockchain."""

import os
import sys
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from src.common.indexer import Indexer
from src.indexers.polymarket.adaptive_chunks import AdaptiveChunkSizer
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
TRADES_HEARTBEAT_SEC = float(os.getenv("TRADES_HEARTBEAT_SEC", "30"))
TRADES_ADAPTIVE_CHUNKS = os.getenv("TRADES_ADAPTIVE_CHUNKS", "1") not in ("0", "false", "False")
TRADES_TARGET_LOGS = int(os.getenv("TRADES_TARGET_LOGS", "8500"))
TRADES_MIN_BLOCKS = int(os.getenv("TRADES_MIN_BLOCKS", "10"))
TRADES_MAX_BLOCKS = int(os.getenv("TRADES_MAX_BLOCKS", "400"))
TRADES_EMA_ALPHA = float(os.getenv("TRADES_EMA_ALPHA", "0.25"))

CONTRACTS = [
    ("CTF Exchange", CTF_EXCHANGE),
    ("NegRisk CTF Exchange", NEGRISK_CTF_EXCHANGE),
]

_tls = threading.local()


def _get_polygon_client() -> PolygonClient:
    client = getattr(_tls, "client", None)
    if client is None:
        client = PolygonClient()
        _tls.client = client
    return client


def _log(msg: str) -> None:
    """Line-buffered status for PowerShell (tqdm alone can look idle for minutes)."""
    tqdm.write(msg, file=sys.stderr)
    sys.stderr.flush()


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


def _fetch_contract_rows(chunk_start: int, chunk_end: int, contract_name: str, contract_address: str) -> tuple[list[dict], int]:
    fetched_at = datetime.utcnow()
    chunk_client = _get_polygon_client()
    trades = chunk_client.get_trades(
        from_block=chunk_start,
        to_block=chunk_end,
        contract_address=contract_address,
    )
    rows: list[dict] = []
    for trade in trades:
        trade_dict = asdict(trade)
        trade_dict["maker_asset_id"] = str(trade_dict["maker_asset_id"])
        trade_dict["taker_asset_id"] = str(trade_dict["taker_asset_id"])
        trade_dict["_fetched_at"] = fetched_at
        trade_dict["_contract"] = contract_name
        rows.append(trade_dict)
    return rows, len(trades)


def _fetch_chunk_once(chunk_start: int, chunk_end: int) -> tuple[list[dict], int]:
    chunk_rows: list[dict] = []
    peak_logs = 0
    with ThreadPoolExecutor(max_workers=2) as contract_pool:
        futures = [
            contract_pool.submit(_fetch_contract_rows, chunk_start, chunk_end, contract_name, contract_address)
            for contract_name, contract_address in CONTRACTS
        ]
        for future in futures:
            rows, log_count = future.result()
            chunk_rows.extend(rows)
            peak_logs = max(peak_logs, log_count)
    return chunk_rows, peak_logs


def _fetch_chunk_trades(chunk_start: int, chunk_end: int) -> tuple[list[dict], int]:
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
                left_rows, left_peak = _fetch_chunk_trades(chunk_start, mid)
                right_rows, right_peak = _fetch_chunk_trades(mid + 1, chunk_end)
                return left_rows + right_rows, max(left_peak, right_peak)
            if not _is_transient_rpc_error(exc):
                raise
            wait_s = min(120.0, 2.0**attempt)
            tqdm.write(
                f"Chunk {chunk_start}-{chunk_end} transient error ({exc}); "
                f"retry {attempt + 1}/{TRADES_CHUNK_RETRIES} in {wait_s:.0f}s"
            )
            time.sleep(wait_s)
    raise last_err  # type: ignore[misc]


class PolymarketTradesIndexer(Indexer):
    """Fetches and stores Polymarket trades from the Polygon blockchain."""

    def __init__(
        self,
        from_block: Optional[int] = None,
        to_block: Optional[int] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        adaptive_chunks: Optional[bool] = None,
    ):
        super().__init__(
            name="polymarket_trades",
            description="Backfills Polymarket trades from Polygon blockchain to parquet files",
        )
        self._from_block = from_block
        self._to_block = to_block
        self._chunk_size = chunk_size
        self._adaptive_chunks = TRADES_ADAPTIVE_CHUNKS if adaptive_chunks is None else adaptive_chunks

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
        print(f"Total blocks: {to_block - from_block + 1:,}")
        if self._adaptive_chunks:
            print(
                "Adaptive chunks: "
                f"target_logs={TRADES_TARGET_LOGS}, "
                f"blocks=[{TRADES_MIN_BLOCKS},{TRADES_MAX_BLOCKS}], "
                f"seed={self._chunk_size}, ema={TRADES_EMA_ALPHA}"
            )
        else:
            print(f"Fixed chunk size: {self._chunk_size} blocks")
        print(
            "Throttle: "
            f"workers={TRADES_MAX_WORKERS}, "
            f"inflight={TRADES_INFLIGHT_CHUNKS}, pause={TRADES_BATCH_PAUSE_SEC}s"
        )

        if self._adaptive_chunks:
            self._run_adaptive(from_block, to_block)
        else:
            self._run_fixed(from_block, to_block)

    def _save_chunk(self, chunk_start: int, chunk_end: int, trades_batch: list[dict]) -> int:
        if not trades_batch:
            return 0
        chunk_path = DATA_DIR / f"trades_{chunk_start}_{chunk_end}.parquet"
        pq.write_table(pa.Table.from_pylist(trades_batch), chunk_path, compression="snappy")
        tqdm.write(f"Saved {len(trades_batch)} trades to {chunk_path.name}", file=sys.stderr)
        return len(trades_batch)

    def _run_fixed(self, from_block: int, to_block: int) -> None:
        ranges: list[tuple[int, int]] = []
        current = from_block
        while current <= to_block:
            end = min(current + self._chunk_size - 1, to_block)
            ranges.append((current, end))
            current = end + 1

        total_chunks = len(ranges)
        _log(
            f"Queued {total_chunks:,} fixed chunks ({self._chunk_size} blocks each); "
            f"first progress line may take 1-3 min while RPC warms up"
        )

        submit_idx = 0

        def prime(submit_fn: Callable[[int, int], None]) -> None:
            nonlocal submit_idx
            while submit_idx < len(ranges) and len(state["in_flight"]) < TRADES_INFLIGHT_CHUNKS:
                chunk_start, chunk_end = ranges[submit_idx]
                submit_fn(chunk_start, chunk_end)
                submit_idx += 1

        def after_commit(_chunk_start: int, _chunk_end: int, _peak_logs: int, submit_fn: Callable[[int, int], None]) -> None:
            nonlocal submit_idx
            while submit_idx < len(ranges) and len(state["in_flight"]) < TRADES_INFLIGHT_CHUNKS:
                chunk_start, chunk_end = ranges[submit_idx]
                submit_fn(chunk_start, chunk_end)
                submit_idx += 1

        state = {"in_flight": {}}
        self._drive_backfill(
            from_block=from_block,
            to_block=to_block,
            state=state,
            pbar_total=total_chunks,
            pbar_unit=" chunks",
            progress_step=lambda start, end: 1,
            prime=prime,
            after_commit=after_commit,
        )

    def _run_adaptive(self, from_block: int, to_block: int) -> None:
        sizer = AdaptiveChunkSizer(
            target_logs=TRADES_TARGET_LOGS,
            min_blocks=TRADES_MIN_BLOCKS,
            max_blocks=TRADES_MAX_BLOCKS,
            initial_blocks=self._chunk_size,
            ema_alpha=TRADES_EMA_ALPHA,
        )
        _log(
            f"Adaptive scheduler ready (seed={sizer.current_blocks} blocks, "
            f"target={TRADES_TARGET_LOGS} peak logs/contract)"
        )

        next_plan = from_block

        def prime(submit_fn: Callable[[int, int], None]) -> None:
            nonlocal next_plan
            while next_plan <= to_block and len(state["in_flight"]) < TRADES_INFLIGHT_CHUNKS:
                chunk_start, chunk_end = sizer.plan(next_plan, to_block)
                submit_fn(chunk_start, chunk_end)
                next_plan = chunk_end + 1

        def after_commit(chunk_start: int, chunk_end: int, peak_logs: int, submit_fn: Callable[[int, int], None]) -> None:
            nonlocal next_plan
            blocks = chunk_end - chunk_start + 1
            old_blocks = sizer.current_blocks
            sizer.observe(blocks, peak_logs)
            if sizer.current_blocks != old_blocks:
                _log(
                    f"Adaptive span {old_blocks} -> {sizer.current_blocks} blocks "
                    f"(last={blocks}, peak_logs={peak_logs})"
                )
            while next_plan <= to_block and len(state["in_flight"]) < TRADES_INFLIGHT_CHUNKS:
                chunk_start, chunk_end = sizer.plan(next_plan, to_block)
                submit_fn(chunk_start, chunk_end)
                next_plan = chunk_end + 1

        state = {"in_flight": {}}
        self._drive_backfill(
            from_block=from_block,
            to_block=to_block,
            state=state,
            pbar_total=to_block - from_block + 1,
            pbar_unit=" blocks",
            progress_step=lambda start, end: end - start + 1,
            prime=prime,
            after_commit=after_commit,
        )

    def _drive_backfill(
        self,
        *,
        from_block: int,
        to_block: int,
        state: dict,
        pbar_total: int,
        pbar_unit: str,
        progress_step: Callable[[int, int], int],
        prime: Callable[[Callable[[int, int], None]], None],
        after_commit: Callable[[int, int, int, Callable[[int, int], None]], None],
    ) -> None:
        total_saved = 0
        fetched_count = 0
        next_commit = from_block
        pending: dict[int, tuple[int, int, list[dict], int]] = {}
        last_heartbeat = time.monotonic()
        interrupted = False

        pbar = tqdm(
            total=pbar_total,
            desc="Backfilling",
            unit=pbar_unit,
            file=sys.stderr,
            mininterval=1.0,
            dynamic_ncols=True,
        )

        def submit_fn(chunk_start: int, chunk_end: int) -> None:
            future = chunk_pool.submit(_fetch_chunk_trades, chunk_start, chunk_end)
            state["in_flight"][future] = (chunk_start, chunk_end)

        try:
            with ThreadPoolExecutor(max_workers=TRADES_MAX_WORKERS) as chunk_pool:
                prime(submit_fn)
                if state["in_flight"]:
                    _log(f"RPC fetch started: {len(state['in_flight'])} chunks in flight")

                while state["in_flight"]:
                    done, _ = wait(state["in_flight"].keys(), return_when=FIRST_COMPLETED, timeout=TRADES_HEARTBEAT_SEC)
                    if not done:
                        now = time.monotonic()
                        if now - last_heartbeat >= TRADES_HEARTBEAT_SEC:
                            last_heartbeat = now
                            _log(
                                f"Still fetching… in_flight={len(state['in_flight'])} "
                                f"fetched={fetched_count} committed_from={next_commit}"
                            )
                        continue

                    last_heartbeat = time.monotonic()
                    for future in done:
                        chunk_start, chunk_end = state["in_flight"].pop(future)
                        chunk_rows, peak_logs = future.result()
                        fetched_count += 1
                        _log(
                            f"Fetched {chunk_start}-{chunk_end}: {len(chunk_rows)} trades "
                            f"(peak_logs={peak_logs}, commit_from={next_commit})"
                        )
                        pending[chunk_start] = (chunk_start, chunk_end, chunk_rows, peak_logs)

                        while next_commit in pending:
                            c_start, c_end, c_rows, c_peak = pending.pop(next_commit)
                            saved = self._save_chunk(c_start, c_end, c_rows)
                            total_saved += saved
                            pbar.update(progress_step(c_start, c_end))
                            pbar.set_postfix(block=c_end, saved=total_saved, last=saved, span=c_end - c_start + 1)
                            CURSOR_FILE.write_text(str(c_end))
                            next_commit = c_end + 1
                            after_commit(c_start, c_end, c_peak, submit_fn)
                            if TRADES_BATCH_PAUSE_SEC > 0:
                                time.sleep(TRADES_BATCH_PAUSE_SEC)

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
