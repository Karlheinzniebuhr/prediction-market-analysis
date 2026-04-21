"""
build_history_pma.py — Convert raw prediction-market-analysis Parquet data into
the history_pma warehouse format used by the Hivemind anomaly study pipeline.

Reads:
  data/polymarket/markets/*.parquet   (market metadata)
  data/polymarket/trades/*.parquet    (CTF/NegRisk OrderFilled events)
  data/polymarket/blocks/*.parquet    (block_number -> timestamp mapping)

Writes:
  <output>/warehouse/dim/markets.parquet
  <output>/warehouse/dim/tokens.parquet
  <output>/warehouse/facts/trade_legs/date=YYYY-MM-DD/part-NNNNNNNN.parquet
  <output>/state/checkpoint.json
  <output>/state/registry_meta.json
  <output>/state/import_progress.json

Usage:
  python scripts/build_history_pma.py --source-root data/polymarket --output-root data/history_pma
  python scripts/build_history_pma.py --source-root E:/prediction-market-analysis/data/polymarket --output-root E:/history_pma
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Constants (mirroring the TypeScript originals)
# ---------------------------------------------------------------------------
COLLATERAL_ASSET_ID = "0"
SHARE_DECIMALS = 1e6
USDC_DECIMALS = 1e6
SOURCE_LABEL = "pma_adapter_python_v1"

PARQUET_CHUNK_ROWS = 50_000

CRYPTO_KEYWORDS_RE = re.compile(
    r"\b(btc|bitcoin|eth|ethereum|sol|solana|xrp|crypto|doge|dogecoin|"
    r"ada|cardano|matic|polygon|arb|arbitrum|op|optimism)\b",
    re.IGNORECASE,
)

UPDOWN_DURATION_RE = re.compile(r"-updown-(\w+)-", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_crypto_market(question: str) -> bool:
    return bool(CRYPTO_KEYWORDS_RE.search(question))


def is_excluded_updown(slug: str, excluded_durations: set[str]) -> bool:
    if not excluded_durations or "updown" not in slug.lower():
        return False
    m = UPDOWN_DURATION_RE.search(slug.lower())
    return m is not None and m.group(1) in excluded_durations


def safe_lower(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip().lower()


def parse_json_array(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if x is not None and str(x).strip()]
    if v is None:
        return []
    raw = str(v).strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if x is not None and str(x).strip()]
    except (json.JSONDecodeError, TypeError):
        pass
    return []


def utc_date_from_ts_sec(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%Y-%m-%d")


def stable_sha256(obj: Any) -> str:
    raw = json.dumps(obj, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def parse_iso_or_none(v: Any) -> str | None:
    if v is None:
        return None
    raw = str(v).strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return None


def parse_pma_range_file(filepath: Path, prefix: str) -> tuple[int, int] | None:
    m = re.match(rf"^{prefix}_(\d+)_(\d+)\.parquet$", filepath.name.lower())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024**2:
        return f"{n / 1024:.1f} KiB"
    if n < 1024**3:
        return f"{n / 1024**2:.1f} MiB"
    return f"{n / 1024**3:.2f} GiB"


# ---------------------------------------------------------------------------
# Phase 1: Build market + token dimension tables
# ---------------------------------------------------------------------------

def build_registry(
    source_root: Path,
    output_root: Path,
    excluded_durations: set[str],
) -> dict[str, tuple[str, int, str]]:
    """
    Read raw PMA market parquets, filter to crypto, build dim tables.
    Returns token_map: {tokenId -> (conditionId, outcomeIndex, outcome)}.
    """
    markets_dir = source_root / "markets"
    market_files = sorted(markets_dir.glob("**/*.parquet"))
    if not market_files:
        raise FileNotFoundError(f"No market parquet files under {markets_dir}")

    print(f"[registry] reading {len(market_files)} market parquet files ...")

    con = duckdb.connect()
    globs = str(markets_dir / "**" / "*.parquet").replace("\\", "/")
    raw = con.sql(f"""
        SELECT * FROM read_parquet('{globs}', union_by_name=true)
    """).fetchdf()
    con.close()

    seen_condition: set[str] = set()
    seen_token: set[str] = set()
    market_rows: list[dict] = []
    token_rows: list[dict] = []
    token_map: dict[str, tuple[str, int, str]] = {}

    for _, row in raw.iterrows():
        cond = safe_lower(row.get("condition_id") or row.get("conditionId"))
        if not cond or cond in seen_condition:
            continue
        seen_condition.add(cond)

        question = str(row.get("question") or "").strip()
        slug = str(row.get("slug") or "").strip()
        if not is_crypto_market(question):
            continue

        excluded = is_excluded_updown(slug, excluded_durations)
        end_date = row.get("end_date") or row.get("endDate")
        active = bool(row.get("active", False))
        closed = bool(row.get("closed", False))

        market_rows.append({
            "conditionId": cond,
            "slug": slug,
            "question": question,
            "endDateIso": parse_iso_or_none(end_date),
            "active": active,
            "closed": closed,
            "category": "crypto",
            "isExcludedShortTerm": excluded,
            "registryVersion": 1,
        })

        if excluded:
            continue

        outcomes = parse_json_array(row.get("outcomes"))
        token_ids_raw = row.get("clob_token_ids") or row.get("clobTokenIds")
        token_ids = parse_json_array(token_ids_raw)

        for i, tid_raw in enumerate(token_ids):
            tid = safe_lower(tid_raw)
            if not tid or tid in seen_token:
                continue
            seen_token.add(tid)
            outcome = outcomes[i] if i < len(outcomes) else (
                "Yes" if i == 0 else "No" if i == 1 else f"Outcome{i}"
            )
            token_rows.append({
                "tokenId": tid,
                "conditionId": cond,
                "outcomeIndex": i,
                "outcome": outcome,
                "registryVersion": 1,
            })
            token_map[tid] = (cond, i, outcome)

    market_rows.sort(key=lambda r: r["conditionId"])
    token_rows.sort(key=lambda r: r["tokenId"])

    dim_dir = output_root / "warehouse" / "dim"
    dim_dir.mkdir(parents=True, exist_ok=True)

    markets_table = pa.Table.from_pylist(market_rows, schema=pa.schema([
        ("conditionId", pa.string()),
        ("slug", pa.string()),
        ("question", pa.string()),
        ("endDateIso", pa.string()),
        ("active", pa.bool_()),
        ("closed", pa.bool_()),
        ("category", pa.string()),
        ("isExcludedShortTerm", pa.bool_()),
        ("registryVersion", pa.int32()),
    ]))
    pq.write_table(markets_table, dim_dir / "markets.parquet")

    tokens_table = pa.Table.from_pylist(token_rows, schema=pa.schema([
        ("tokenId", pa.string()),
        ("conditionId", pa.string()),
        ("outcomeIndex", pa.int32()),
        ("outcome", pa.string()),
        ("registryVersion", pa.int32()),
    ]))
    pq.write_table(tokens_table, dim_dir / "tokens.parquet")

    state_dir = output_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "schemaVersion": 1,
        "registryVersion": 1,
        "createdAtIso": datetime.now(timezone.utc).isoformat(),
        "filters": {
            "category": "crypto",
            "excludedUpdownDurations": sorted(excluded_durations),
        },
        "marketsHash": stable_sha256(market_rows),
        "tokensHash": stable_sha256(token_rows),
        "marketCount": len(market_rows),
        "tokenCount": len(token_rows),
    }
    write_json(state_dir / "registry_meta.json", meta)

    print(f"[registry] {len(market_rows)} crypto markets, {len(token_rows)} tokens, "
          f"{len(token_map)} mapped token IDs")
    return token_map


# ---------------------------------------------------------------------------
# Phase 2: Block timestamp resolver
# ---------------------------------------------------------------------------

class BlockTimestampResolver:
    """
    Resolves block numbers to Unix timestamps by reading PMA blocks parquet files.
    Uses an LRU cache to avoid reloading the same file repeatedly.
    """

    def __init__(self, blocks_dir: Path, max_cached: int = 6):
        self._ranges: list[tuple[int, int, Path]] = []
        self._cache: dict[str, dict[int, int]] = {}
        self._order: list[str] = []
        self._max_cached = max(1, max_cached)
        self._last_idx = -1

        for fp in sorted(blocks_dir.glob("**/*.parquet")):
            parsed = parse_pma_range_file(fp, "blocks")
            if parsed:
                self._ranges.append((parsed[0], parsed[1], fp))
        self._ranges.sort(key=lambda r: (r[0], r[1]))
        print(f"[blocks] indexed {len(self._ranges)} block range files")

    def _locate(self, block_number: int) -> int:
        if (
            0 <= self._last_idx < len(self._ranges)
            and self._ranges[self._last_idx][0] <= block_number < self._ranges[self._last_idx][1]
        ):
            return self._last_idx
        lo, hi = 0, len(self._ranges) - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            s, e, _ = self._ranges[mid]
            if block_number < s:
                hi = mid - 1
            elif block_number >= e:
                lo = mid + 1
            else:
                self._last_idx = mid
                return mid
        return -1

    def _load(self, fp: Path) -> dict[int, int]:
        key = str(fp)
        if key in self._cache:
            return self._cache[key]

        table = pq.read_table(fp)
        block_col = table.column("block_number").to_pylist()
        ts_col = table.column("timestamp").to_pylist()
        block_map: dict[int, int] = {}
        for bn, ts in zip(block_col, ts_col):
            bn_int = int(bn) if bn is not None else -1
            if bn_int < 0:
                continue
            ts_val = ts
            if isinstance(ts_val, str):
                try:
                    dt = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
                    ts_sec = int(dt.timestamp())
                except (ValueError, TypeError):
                    continue
            elif isinstance(ts_val, (int, float)):
                ts_sec = int(ts_val) if ts_val < 1e12 else int(ts_val / 1000)
            else:
                continue
            if ts_sec > 0:
                block_map[bn_int] = ts_sec

        self._cache[key] = block_map
        self._order.append(key)
        while len(self._order) > self._max_cached:
            oldest = self._order.pop(0)
            self._cache.pop(oldest, None)
        return block_map

    def get_ts_sec(self, block_number: int) -> int | None:
        if not math.isfinite(block_number) or block_number < 0:
            return None
        idx = self._locate(int(block_number))
        if idx < 0:
            return None
        _, _, fp = self._ranges[idx]
        block_map = self._load(fp)
        return block_map.get(int(block_number))


# ---------------------------------------------------------------------------
# Phase 3: Trade conversion — raw PMA trade -> two trade legs
# ---------------------------------------------------------------------------

TRADE_LEG_SCHEMA = pa.schema([
    ("fillId", pa.string()),
    ("txHash", pa.string()),
    ("orderHash", pa.string()),
    ("tsSec", pa.int64()),
    ("wallet", pa.string()),
    ("counterparty", pa.string()),
    ("role", pa.string()),
    ("tokenId", pa.string()),
    ("conditionId", pa.string()),
    ("outcomeIndex", pa.int32()),
    ("side", pa.string()),
    ("sharesRaw", pa.string()),
    ("usdcRaw", pa.string()),
    ("feeRaw", pa.string()),
    ("shares", pa.float64()),
    ("usdc", pa.float64()),
    ("feeUsdc", pa.float64()),
    ("price", pa.float64()),
    ("registryVersion", pa.int32()),
    ("source", pa.string()),
])


def to_trade_legs(
    row: dict[str, Any],
    ts_sec: int,
    token_info: tuple[str, int, str],
) -> list[dict[str, Any]] | None:
    """Convert a raw PMA trade row into two trade legs (maker + taker)."""
    tx_hash = safe_lower(row.get("transaction_hash") or row.get("transactionHash") or "")
    order_hash = safe_lower(row.get("order_hash") or row.get("orderHash") or "")
    maker = safe_lower(row.get("maker") or "")
    taker = safe_lower(row.get("taker") or "")
    maker_asset_id = safe_lower(row.get("maker_asset_id") or row.get("makerAssetId") or "")
    taker_asset_id = safe_lower(row.get("taker_asset_id") or row.get("takerAssetId") or "")
    maker_amount_raw = str(row.get("maker_amount") or row.get("makerAmount") or "").strip()
    taker_amount_raw = str(row.get("taker_amount") or row.get("takerAmount") or "").strip()
    fee_raw = str(row.get("fee") or "").strip()
    log_index = row.get("log_index") or row.get("logIndex")
    contract_raw = re.sub(r"[^a-z0-9]+", "-", safe_lower(row.get("_contract") or ""))

    if not tx_hash or not maker or not taker or not maker_amount_raw or not taker_amount_raw:
        return None

    fill_suffix = str(int(log_index)) if log_index is not None else "0"
    fill_id = f"{tx_hash}_{fill_suffix}_{contract_raw}" if contract_raw else f"{tx_hash}_{fill_suffix}"

    cond_id, outcome_idx, _outcome = token_info
    token_id_for_leg = ""
    for candidate in [maker_asset_id, taker_asset_id]:
        if candidate and candidate != COLLATERAL_ASSET_ID:
            token_id_for_leg = candidate
            break

    maker_sells_collateral = maker_asset_id == COLLATERAL_ASSET_ID
    if maker_sells_collateral:
        shares_raw = taker_amount_raw
        usdc_raw = maker_amount_raw
        maker_side = "buy"
        taker_side = "sell"
    elif taker_asset_id == COLLATERAL_ASSET_ID:
        shares_raw = maker_amount_raw
        usdc_raw = taker_amount_raw
        maker_side = "sell"
        taker_side = "buy"
    else:
        return None

    try:
        shares = float(shares_raw) / SHARE_DECIMALS
        usdc = float(usdc_raw) / USDC_DECIMALS
    except (ValueError, TypeError):
        return None

    if not math.isfinite(shares) or shares <= 0 or not math.isfinite(usdc) or usdc < 0:
        return None

    try:
        fee_usdc = float(fee_raw) / USDC_DECIMALS if fee_raw else 0.0
    except (ValueError, TypeError):
        fee_usdc = 0.0

    price = usdc / shares if shares > 0 else 0.0
    if not math.isfinite(price) or price <= 0:
        return None

    base = {
        "fillId": fill_id,
        "txHash": tx_hash,
        "orderHash": order_hash,
        "tsSec": ts_sec,
        "tokenId": token_id_for_leg,
        "conditionId": cond_id,
        "outcomeIndex": outcome_idx,
        "sharesRaw": shares_raw,
        "usdcRaw": usdc_raw,
        "shares": shares,
        "usdc": usdc,
        "price": price,
        "registryVersion": 1,
        "source": SOURCE_LABEL,
    }

    return [
        {**base, "wallet": maker, "counterparty": taker, "role": "maker",
         "side": maker_side, "feeRaw": fee_raw or "", "feeUsdc": fee_usdc},
        {**base, "wallet": taker, "counterparty": maker, "role": "taker",
         "side": taker_side, "feeRaw": "", "feeUsdc": 0.0},
    ]


# ---------------------------------------------------------------------------
# Phase 4: Process trade files
# ---------------------------------------------------------------------------

def list_trade_chunk_files(trades_dir: Path) -> list[tuple[int, int, Path]]:
    """List and sort trade parquet files by block range, skipping legacy."""
    chunks: list[tuple[int, int, Path]] = []
    for fp in trades_dir.glob("**/*.parquet"):
        norm = str(fp).replace("\\", "/").lower()
        if "legacy_trades" in norm or "legacy/trades" in norm:
            continue
        parsed = parse_pma_range_file(fp, "trades")
        if parsed:
            chunks.append((parsed[0], parsed[1], fp))
    chunks.sort(key=lambda c: (c[0], c[1]))
    return chunks


def flush_legs(
    legs_buffer: list[dict[str, Any]],
    trade_legs_dir: Path,
    part_counter: int,
) -> tuple[int, int]:
    """Write buffered legs to date-partitioned parquet. Returns (bytes_written, new_part_counter)."""
    if not legs_buffer:
        return 0, part_counter

    by_day: dict[str, list[dict]] = {}
    for leg in legs_buffer:
        day = utc_date_from_ts_sec(leg["tsSec"])
        by_day.setdefault(day, []).append(leg)

    total_bytes = 0
    for day in sorted(by_day):
        part_counter += 1
        day_dir = trade_legs_dir / f"date={day}"
        day_dir.mkdir(parents=True, exist_ok=True)
        fp = day_dir / f"part-{part_counter:08d}.parquet"
        table = pa.Table.from_pylist(by_day[day], schema=TRADE_LEG_SCHEMA)
        pq.write_table(table, fp, compression="snappy")
        total_bytes += fp.stat().st_size

    return total_bytes, part_counter


def process_trades(
    source_root: Path,
    output_root: Path,
    token_map: dict[str, tuple[str, int, str]],
    block_resolver: BlockTimestampResolver,
    *,
    max_files: int = 0,
) -> dict[str, Any]:
    """Main trade processing loop."""
    trades_dir = source_root / "trades"
    trade_files = list_trade_chunk_files(trades_dir)
    if not trade_files:
        raise FileNotFoundError(f"No trade parquet files under {trades_dir}")

    total_files = len(trade_files)
    if max_files > 0:
        trade_files = trade_files[:max_files]
    print(f"[trades] {len(trade_files)} of {total_files} trade files to process")

    trade_legs_dir = output_root / "warehouse" / "facts" / "trade_legs"
    trade_legs_dir.mkdir(parents=True, exist_ok=True)

    part_counter = 0
    buffer: list[dict[str, Any]] = []
    stats = {
        "tradeFilesTotal": total_files,
        "tradeFilesProcessed": 0,
        "tradeRowsRead": 0,
        "fillsMapped": 0,
        "legsWritten": 0,
        "bytesWritten": 0,
        "missingTokenMappings": 0,
        "missingBlockTimestamps": 0,
        "invalidRows": 0,
    }

    t0 = time.time()
    for fi, (_, _, fp) in enumerate(trade_files):
        table = pq.read_table(fp)
        rows = table.to_pydict()
        n_rows = table.num_rows
        stats["tradeRowsRead"] += n_rows
        stats["tradeFilesProcessed"] += 1

        col_names = table.column_names
        for ri in range(n_rows):
            row = {c: rows[c][ri] for c in col_names}

            maker_aid = safe_lower(row.get("maker_asset_id") or row.get("makerAssetId") or "")
            taker_aid = safe_lower(row.get("taker_asset_id") or row.get("takerAssetId") or "")
            token_info = token_map.get(maker_aid) or token_map.get(taker_aid)
            if not token_info:
                stats["missingTokenMappings"] += 1
                continue

            block_num = row.get("block_number") or row.get("blockNumber")
            ts_sec = None
            if block_num is not None:
                try:
                    ts_sec = block_resolver.get_ts_sec(int(block_num))
                except (ValueError, TypeError):
                    pass

            if ts_sec is None:
                ts_raw = row.get("timestamp")
                if ts_raw is not None:
                    try:
                        ts_val = float(ts_raw) if not isinstance(ts_raw, str) else None
                        if ts_val and math.isfinite(ts_val):
                            ts_sec = int(ts_val) if ts_val < 1e12 else int(ts_val / 1000)
                        elif isinstance(ts_raw, str):
                            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                            ts_sec = int(dt.timestamp())
                    except (ValueError, TypeError):
                        pass

            if ts_sec is None or ts_sec <= 0:
                stats["missingBlockTimestamps"] += 1
                continue

            legs = to_trade_legs(row, ts_sec, token_info)
            if not legs:
                stats["invalidRows"] += 1
                continue

            stats["fillsMapped"] += 1
            buffer.extend(legs)

            if len(buffer) >= PARQUET_CHUNK_ROWS:
                bytes_written, part_counter = flush_legs(buffer, trade_legs_dir, part_counter)
                stats["legsWritten"] += len(buffer)
                stats["bytesWritten"] += bytes_written
                buffer.clear()

        elapsed = time.time() - t0
        rate = stats["fillsMapped"] / max(1, elapsed)
        print(
            f"[trades] {fi + 1}/{len(trade_files)} files | "
            f"{stats['fillsMapped']:,} fills | {stats['legsWritten']:,} legs | "
            f"{fmt_bytes(stats['bytesWritten'])} | {rate:.0f} fills/s"
        )

    if buffer:
        bytes_written, part_counter = flush_legs(buffer, trade_legs_dir, part_counter)
        stats["legsWritten"] += len(buffer)
        stats["bytesWritten"] += bytes_written
        buffer.clear()

    state_dir = output_root / "state"
    write_json(state_dir / "checkpoint.json", {
        "schemaVersion": 1,
        "registryVersion": 1,
        "rowsWritten": stats["legsWritten"],
        "bytesWritten": stats["bytesWritten"],
        "partCounter": part_counter,
    })
    write_json(state_dir / "import_progress.json", {
        "files_processed": stats["tradeFilesProcessed"],
        "legs_written": stats["legsWritten"],
    })

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build history_pma warehouse from raw prediction-market-analysis data"
    )
    parser.add_argument(
        "--source-root", required=True,
        help="Path to PMA polymarket data (contains markets/, trades/, blocks/)"
    )
    parser.add_argument(
        "--output-root", required=True,
        help="Output path for the warehouse (e.g., data/history_pma)"
    )
    parser.add_argument(
        "--exclude-durations", default="",
        help="Comma-separated updown durations to exclude (default: none — include all)"
    )
    parser.add_argument(
        "--max-trade-files", type=int, default=0,
        help="Limit number of trade files to process (0=all, useful for testing)"
    )
    parser.add_argument(
        "--block-cache-files", type=int, default=6,
        help="Max block timestamp files to keep in memory (default: 6)"
    )
    args = parser.parse_args()

    source_root = Path(args.source_root).resolve()
    output_root = Path(args.output_root).resolve()

    if not (source_root / "markets").is_dir():
        for candidate in [source_root / "data" / "polymarket", source_root]:
            if (candidate / "markets").is_dir():
                source_root = candidate
                break
        else:
            print(f"ERROR: Cannot find markets/ directory under {args.source_root}", file=sys.stderr)
            sys.exit(1)

    excluded = {
        d.strip().lower()
        for d in args.exclude_durations.split(",")
        if d.strip()
    }

    print(f"[config] source:  {source_root}")
    print(f"[config] output:  {output_root}")
    print(f"[config] exclude: {sorted(excluded)}")
    print()

    t_start = time.time()

    token_map = build_registry(source_root, output_root, excluded)
    if not token_map:
        print("ERROR: Token map is empty after filtering. Check source data.", file=sys.stderr)
        sys.exit(1)

    block_resolver = BlockTimestampResolver(
        source_root / "blocks",
        max_cached=args.block_cache_files,
    )

    stats = process_trades(
        source_root, output_root, token_map, block_resolver,
        max_files=args.max_trade_files,
    )

    elapsed = time.time() - t_start
    print()
    print("=" * 60)
    print(f"Build complete in {elapsed:.1f}s")
    print(f"  Trade files processed: {stats['tradeFilesProcessed']:,} / {stats['tradeFilesTotal']:,}")
    print(f"  Trade rows read:       {stats['tradeRowsRead']:,}")
    print(f"  Fills mapped:          {stats['fillsMapped']:,}")
    print(f"  Legs written:          {stats['legsWritten']:,}")
    print(f"  Bytes written:         {fmt_bytes(stats['bytesWritten'])}")
    print(f"  Missing token maps:    {stats['missingTokenMappings']:,}")
    print(f"  Missing block ts:      {stats['missingBlockTimestamps']:,}")
    print(f"  Invalid rows:          {stats['invalidRows']:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
