"""Probe Polygon RPC to find safe trades-indexer throttle settings."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from dotenv import load_dotenv

from src.indexers.polymarket.blockchain import CTF_EXCHANGE, NEGRISK_CTF_EXCHANGE, PolygonClient

load_dotenv()

LOG_LIMIT = 10_000
TARGET_LOGS = int(os.getenv("PROBE_TARGET_LOGS", "8500"))
DRPC_CU_PER_CALL = 20
DRPC_CU_PER_SEC_NORMAL = 2_000
DRPC_CU_PER_SEC_STRESS = 840


@dataclass
class ChunkProbe:
    blocks: int
    ctf_logs: int
    negrisk_logs: int
    max_logs: int
    latency_s: float
    ok: bool
    error: str | None = None


@dataclass
class ConcurrencyProbe:
    workers: int
    requests: int
    errors: int
    error_rate: float
    req_per_sec: float
    cu_per_sec: float
    p50_latency_s: float
    p95_latency_s: float



def probe_chunk_sizes(start_block: int, sizes: list[int]) -> list[ChunkProbe]:
    client = PolygonClient()
    results: list[ChunkProbe] = []

    for blocks in sizes:
        end = start_block + blocks - 1
        t0 = time.perf_counter()
        err: str | None = None
        ctf_logs = negrisk_logs = 0
        ok = True
        try:
            ctf_trades = client.get_trades(start_block, end, CTF_EXCHANGE)
            ctf_logs = len(ctf_trades)
            negrisk_trades = client.get_trades(start_block, end, NEGRISK_CTF_EXCHANGE)
            negrisk_logs = len(negrisk_trades)
        except Exception as exc:
            ok = False
            err = str(exc)
        latency = time.perf_counter() - t0
        max_logs = max(ctf_logs, negrisk_logs)
        results.append(
            ChunkProbe(
                blocks=blocks,
                ctf_logs=ctf_logs,
                negrisk_logs=negrisk_logs,
                max_logs=max_logs,
                latency_s=latency,
                ok=ok,
                error=err,
            )
        )
        print(
            f"  blocks={blocks:4d}  max_logs={max_logs:5d}  "
            f"ctf={ctf_logs:5d}  negrisk={negrisk_logs:5d}  "
            f"latency={latency:5.2f}s  ok={ok}"
            + (f"  err={err[:80]}" if err else "")
        )
        time.sleep(0.25)
    return results


def find_max_safe_blocks(start_block: int) -> int:
    """Binary search largest span that stays under TARGET_LOGS per contract."""
    client = PolygonClient()
    lo, hi = 10, 400
    best = 10

    while lo <= hi:
        mid = (lo + hi) // 2
        end = start_block + mid - 1
        try:
            ctf = len(client.get_trades(start_block, end, CTF_EXCHANGE))
            negrisk = len(client.get_trades(start_block, end, NEGRISK_CTF_EXCHANGE))
            peak = max(ctf, negrisk)
            if peak <= TARGET_LOGS:
                best = mid
                lo = mid + 1
            else:
                hi = mid - 1
        except Exception:
            hi = mid - 1
        time.sleep(0.15)

    return best


def _fetch_pair(start: int, blocks: int) -> tuple[float, bool]:
    client = PolygonClient()
    end = start + blocks - 1
    t0 = time.perf_counter()
    try:
        client.get_trades(start, end, CTF_EXCHANGE)
        client.get_trades(start, end, NEGRISK_CTF_EXCHANGE)
        return time.perf_counter() - t0, False
    except Exception:
        return time.perf_counter() - t0, True


def probe_concurrency(start_block: int, chunk_blocks: int, workers_list: list[int], duration_sec: float = 20.0) -> list[ConcurrencyProbe]:
    results: list[ConcurrencyProbe] = []
    span = chunk_blocks

    for workers in workers_list:
        latencies: list[float] = []
        errors = 0
        requests = 0
        t_end = time.perf_counter() + duration_sec
        offset = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = []
            while time.perf_counter() < t_end:
                while len(futures) < workers and time.perf_counter() < t_end:
                    block_start = start_block + offset * span
                    futures.append(pool.submit(_fetch_pair, block_start, span))
                    offset += 1
                if not futures:
                    break
                done = next(as_completed(futures))
                futures.remove(done)
                latency, failed = done.result()
                latencies.append(latency)
                requests += 2
                if failed:
                    errors += 1

            for future in as_completed(futures):
                latency, failed = future.result()
                latencies.append(latency)
                requests += 2
                if failed:
                    errors += 1

        elapsed = max(duration_sec, 1e-6)
        req_per_sec = requests / elapsed
        cu_per_sec = req_per_sec * DRPC_CU_PER_CALL
        error_rate = errors / max(1, requests // 2)
        p50 = statistics.median(latencies) if latencies else 0.0
        p95 = sorted(latencies)[int(0.95 * (len(latencies) - 1))] if len(latencies) > 1 else p50
        row = ConcurrencyProbe(
            workers=workers,
            requests=requests,
            errors=errors,
            error_rate=error_rate,
            req_per_sec=req_per_sec,
            cu_per_sec=cu_per_sec,
            p50_latency_s=p50,
            p95_latency_s=p95,
        )
        results.append(row)
        print(
            f"  workers={workers:2d}  rps={req_per_sec:5.1f}  cu/s={cu_per_sec:6.0f}  "
            f"errors={errors:3d}  err_rate={error_rate:5.1%}  p50={p50:.2f}s  p95={p95:.2f}s"
        )
        time.sleep(1.0)
    return results


def recommend(chunk_blocks: int, concurrency: list[ConcurrencyProbe]) -> dict:
    safe = [c for c in concurrency if c.error_rate <= 0.02 and c.cu_per_sec <= DRPC_CU_PER_SEC_STRESS * 0.85]
    if not safe:
        safe = [c for c in concurrency if c.error_rate <= 0.05]
    if not safe:
        safe = sorted(concurrency, key=lambda c: (c.error_rate, -c.req_per_sec))[:1]

    best = max(safe, key=lambda c: c.req_per_sec)
    return {
        "TRADES_CHUNK_SIZE": chunk_blocks,
        "TRADES_MAX_WORKERS": best.workers,
        "TRADES_INFLIGHT_CHUNKS": best.workers,
        "TRADES_BATCH_PAUSE_SEC": 0,
        "probe_meta": {
            "target_logs_per_contract": TARGET_LOGS,
            "drpc_log_limit": LOG_LIMIT,
            "chosen_workers": best.workers,
            "chosen_req_per_sec": round(best.req_per_sec, 2),
            "chosen_cu_per_sec": round(best.cu_per_sec, 2),
            "chosen_error_rate": round(best.error_rate, 4),
            "drpc_normal_cu_per_sec": DRPC_CU_PER_SEC_NORMAL,
            "drpc_stress_cu_per_sec": DRPC_CU_PER_SEC_STRESS,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-block", type=int, default=None)
    parser.add_argument("--output", type=Path, default=Path("logs/trades_rpc_probe.json"))
    args = parser.parse_args()

    cursor_file = Path("data/polymarket/.backfill_block_cursor")
    start_block = args.start_block
    if start_block is None:
        start_block = int(cursor_file.read_text().strip()) if cursor_file.exists() else 82_700_000

    print(f"Probing from block {start_block} via {os.getenv('POLYGON_RPC', '')}")
    print("\n[1/3] Binary search for max safe chunk size...")
    safe_blocks = find_max_safe_blocks(start_block)
    print(f"  -> safe chunk size: {safe_blocks} blocks (target <= {TARGET_LOGS} logs/contract)")

    print("\n[2/3] Spot-check nearby sizes...")
    check_sizes = sorted({max(10, safe_blocks - 25), safe_blocks, min(400, safe_blocks + 25)})
    chunk_rows = probe_chunk_sizes(start_block, check_sizes)

    print("\n[3/3] Concurrency sweep (20s each)...")
    worker_candidates = [4, 8, 12, 16, 20, 24]
    conc_rows = probe_concurrency(start_block, safe_blocks, worker_candidates, duration_sec=20.0)

    settings = recommend(safe_blocks, conc_rows)
    payload = {
        "start_block": start_block,
        "recommended": settings,
        "chunk_probes": [asdict(r) for r in chunk_rows],
        "concurrency_probes": [asdict(r) for r in conc_rows],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print("\nRecommended settings:")
    for key in ("TRADES_CHUNK_SIZE", "TRADES_MAX_WORKERS", "TRADES_INFLIGHT_CHUNKS", "TRADES_BATCH_PAUSE_SEC"):
        print(f"  {key}={settings[key]}")
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()
