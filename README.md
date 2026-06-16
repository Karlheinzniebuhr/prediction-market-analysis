# prediction-market-analysis — ML-ready warehouse fork

**A data transformation layer on top of [Jon Becker's prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis)** — the largest public Polymarket dataset. This fork adds a pure-Python, cross-platform ETL that converts 185M+ raw on-chain fill events into a **research-ready warehouse** of timestamped, wallet-attributed, direction-labeled trade legs in Hive-partitioned Parquet.

No Node.js required. Built for ML pipelines, rolling wallet windows, and fast queries with DuckDB, Polars, and PyArrow.

---

## What this fork adds (vs upstream)

| | **Upstream** ([Jon-Becker/prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis)) | **This fork** |
|---|---|---|
| **Raw data** | Hosted tarball + indexers → `markets/`, `trades/`, `blocks/` Parquet | Same (inherits + extends) |
| **Research warehouse** | — | **`build_history_pma.py`** → date-partitioned `trade_legs` |
| **Trade direction** | Opaque 77-digit token IDs, block numbers | Explicit **`buy` / `sell`**, **`wallet`**, **`tsSec`** |
| **Fill → legs** | One row per on-chain fill | **Two legs per fill** (maker + taker) |
| **Time queries** | Join blocks + decode collateral logic | **Hive `date=YYYY-MM-DD/`** partitions |
| **Crypto filter** | All categories | Pre-filtered **crypto markets** + optional updown exclusions |
| **Windows workflow** | Linux/macOS Makefile focus | **`update_history_pma.ps1`**, resumable **`download.py`**, RPC probe |
| **Extend to today** | Indexers exist | **Phase 2 playbook**: Gamma + Polygon RPC → `--resume` append |

Upstream remains the source of truth for raw archival data, analysis scripts, and Kalshi coverage. **Our contribution is the transformation layer** — turning that archival blockchain data into something ML pipelines can actually consume.

---

## Why this exists

> We're open-sourcing a data transformation layer for on-chain Polymarket trade data.
>
> It converts **185M+ raw blockchain fill events** into a research-ready warehouse of **timestamped, wallet-attributed, direction-labeled trade legs** in date-partitioned Parquet — built for ML, and easy to query with DuckDB, Polars, and PyArrow.

Raw Polymarket data is hard to work with. You get opaque 77-digit token IDs, block numbers instead of timestamps, and no explicit notion of “buy” vs “sell.” Even a simple question like *“what did wallet X trade on March 15?”* requires joining multiple tables and decoding collateral logic. **We automated that entire layer.**

We built this to study **wallet-level behavioral patterns over time for ML models** — rolling windows of per-wallet activity, resolved to real timestamps, with explicit trade direction, recomputed at every time step to avoid look-ahead bias. Raw blockchain events don't give you that out of the box.

The result is **~27 GB of Snappy-compressed Parquet**. Because it's date-partitioned, DuckDB and Polars skip irrelevant partitions automatically. Queries that would otherwise take hours can run in seconds — the difference between *“run overnight”* and *“iterate live in a notebook.”*

---

## The ETL pipeline

`scripts/build_history_pma.py` reads three raw layers from [Becker's dataset](https://github.com/Jon-Becker/prediction-market-analysis) and writes the warehouse:

```
data/polymarket/markets/   ← Gamma API metadata (via polymarket_markets indexer)
data/polymarket/blocks/    ← block_number → timestamp (via polymarket_blocks indexer)
data/polymarket/trades/    ← CTF/NegRisk OrderFilled events (via polymarket_trades indexer)
         │
         ▼  build_history_pma.py
data/history_pma/warehouse/
├── dim/markets.parquet      # ~150K crypto markets
├── dim/tokens.parquet       # ~130K outcome tokens
└── facts/trade_legs/date=YYYY-MM-DD/part-*.parquet
```

### What the builder does

1. **Block → timestamp** — `BlockTimestampResolver` indexes hundreds of block-range Parquet files with binary search + LRU caching (`--block-cache-files`, default 6).
2. **Token ID → market/outcome** — DuckDB scan of Gamma market metadata; builds `dim/markets` and `dim/tokens` registries.
3. **Fill → two trade legs** — each `OrderFilled` event becomes maker + taker rows with `wallet`, `role`, `side`, `shares`, `usdc`, `price`, `feeUsdc`.
4. **Buy/sell decoding** — collateral (`asset_id == 0`) vs outcome token determines direction per leg.
5. **Crypto pre-filter** — keyword match on market question (BTC, ETH, SOL, …); optional updown duration exclusions.
6. **Hive partitioning** — legs written to `date=YYYY-MM-DD/` from resolved `tsSec` for partition pruning.

Processing is resumable: `state/import_progress.json` tracks files and leg counts; `--resume` skips already-imported trade chunks (Phase 2 append).

---

## Warehouse layout

```
data/history_pma/
├── state/
│   ├── checkpoint.json
│   ├── registry_meta.json       # market/token counts, schema version
│   └── import_progress.json     # files processed, legs written
└── warehouse/
    ├── dim/
    │   ├── markets.parquet
    │   └── tokens.parquet
    └── facts/trade_legs/
        ├── date=2023-03-05/part-00000001.parquet
        └── ...
```

### Trade leg schema

| Column | Description |
|--------|-------------|
| `fillId` | Unique fill id (`txHash_logIndex_contract`) |
| `tsSec` | Unix timestamp (from block resolver) |
| `wallet` / `counterparty` | Addresses for this leg |
| `role` | `maker` or `taker` |
| `side` | **`buy` or `sell`** (collateral logic decoded) |
| `tokenId` / `conditionId` / `outcomeIndex` | Market linkage |
| `shares`, `usdc`, `price`, `feeUsdc` | Human-scale amounts |
| `source` | Provenance (`pma_adapter_python_v1`) |

Full column reference and raw schemas: [docs/SCHEMAS.md](docs/SCHEMAS.md).

---

## Quick start

Requires **Python 3.9+** and [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

Override paths for a large drive:

```powershell
$env:HISTORY_PMA_SOURCE_ROOT = "path/to/data/polymarket"
$env:HISTORY_PMA_OUTPUT_ROOT  = "path/to/history_pma"
```

### Phase 1 — bulk refresh (hosted snapshot, ~Jan 2026 cap)

Downloads [Becker's hosted archive](https://s3.jbecker.dev/data.tar.zst) (~36 GiB) and runs a full warehouse build.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/update_history_pma.ps1
```

Manual equivalent:

```powershell
uv run python -u scripts/download.py --force
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations=''
```

Phase 1 alone does **not** pass ~January 2026 — the hosted tarball has not been republished with newer trades.

### Phase 2 — extend to chain head

After Phase 1, backfill raw data via Gamma + Polygon RPC, then append warehouse partitions.

**All four steps** (wrapper):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
```

**Manual** (same commands; each step resumes automatically):

```powershell
cd path/to/prediction-market-analysis
$env:PYTHONUNBUFFERED = "1"

# 1) Markets (Gamma) — cursor: data/polymarket/.backfill_markets_cursor
uv run python -u main.py index polymarket_markets

# 2) Blocks (Polygon RPC)
uv run python -u main.py index polymarket_blocks

# 3) Trades (Polygon RPC, long pole) — cursor: data/polymarket/.backfill_block_cursor
uv run python -u main.py index polymarket_trades

# 4) Append warehouse (skips snapshot files in import_progress.json)
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations='' `
  --resume
```

Trades indexer emits live fetch/save lines to stderr (not just the tqdm bar). Step 3 can run for days on free RPC — tune with `scripts/probe_trades_rpc.py`.

**RPC settings** (`.env`):

```env
POLYGON_RPC=https://polygon.drpc.org
TRADES_CHUNK_SIZE=100
TRADES_MAX_WORKERS=20
TRADES_INFLIGHT_CHUNKS=20
TRADES_BATCH_PAUSE_SEC=0
```

Rebuild only (raw trades already extended):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1 -SkipIndexers
```

### Verify

```powershell
Get-Content data/history_pma/state/import_progress.json
Get-Content data/history_pma/state/registry_meta.json
Get-ChildItem data/history_pma/warehouse/facts/trade_legs -Directory |
  Sort-Object Name -Descending | Select-Object -First 3 Name
Get-Content data/polymarket/.backfill_block_cursor   # gone when trades backfill complete
```

Full checklist: [docs/UPDATE_HISTORY_PMA.md](docs/UPDATE_HISTORY_PMA.md).

---

## Query examples

**DuckDB** — cumulative flow per wallet in a date range:

```sql
SELECT wallet,
       SUM(CASE WHEN side = 'buy' THEN -usdc ELSE usdc END) AS net_flow
FROM read_parquet('data/history_pma/warehouse/facts/trade_legs/**/*.parquet', hive_partitioning=true)
WHERE tsSec BETWEEN 1700000000 AND 1710000000
GROUP BY wallet
ORDER BY net_flow DESC
LIMIT 20;
```

**Polars** — BTC-related legs with market questions:

```python
import polars as pl

legs = pl.scan_parquet("data/history_pma/warehouse/facts/trade_legs/**/*.parquet", hive_partitioning=True)
markets = pl.scan_parquet("data/history_pma/warehouse/dim/markets.parquet")
btc = markets.filter(pl.col("question").str.contains("(?i)btc|bitcoin")).select("conditionId")
df = legs.join(btc.collect(), on="conditionId").collect()
```

---

## Fork-specific tooling

| Script | Purpose |
|--------|---------|
| `scripts/build_history_pma.py` | Raw Parquet → warehouse ETL |
| `scripts/update_history_pma.ps1` | Phase 1: download + full build |
| `scripts/extend_history_pma.ps1` | Phase 2: indexers + `--resume` append |
| `scripts/probe_trades_rpc.py` | Measure safe RPC chunk size / concurrency |
| `scripts/run_phase2_until_done.ps1` | Watchdog: trades until cursor gone, then build |
| `scripts/download.py` | Resumable hosted tarball download + zstd extract |

### Trades indexer hardening (Phase 2)

This fork's `polymarket_trades` indexer adds production-oriented behavior upstream lacks:

- **Measured RPC throttle** — chunk size and worker count from `probe_trades_rpc.py` (drpc log-limit aware)
- **Continuous pipeline** — keeps N chunks in flight (not wave-and-sleep)
- **Split-on-oversize** — bisects ranges that hit RPC payload / 10k log caps
- **Resume at cursor + 1** — no duplicate block on restart
- **Transient retry** — 400s, disconnects, chunked encoding errors backoff instead of exit
- **Live progress** — fetch/heartbeat lines on stderr for long RPC runs

---

## Inherited from upstream

This fork merges [Jon-Becker/prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) and keeps:

- **Analysis framework** — `python main.py analyze`, figures in `output/`
- **Kalshi indexers and analyses**
- **Consolidated `HttpClient`** with rate limiting and retries
- **Raw indexers** — `polymarket_markets`, `polymarket_blocks`, `polymarket_trades`, Kalshi API

```bash
# Interactive analysis menu (Linux/macOS)
make analyze

# Windows / non-interactive
uv run python main.py analyze all
uv run python main.py analyze polymarket_win_rate_by_price
```

See [docs/ANALYSIS.md](docs/ANALYSIS.md) for writing custom analyses.

---

## Project structure

```
├── scripts/
│   ├── build_history_pma.py       # ★ Warehouse ETL (fork)
│   ├── update_history_pma.ps1     # ★ Phase 1 workflow (fork)
│   ├── extend_history_pma.ps1       # ★ Phase 2 workflow (fork)
│   ├── probe_trades_rpc.py          # ★ RPC tuning (fork)
│   ├── download.py                  # Resumable tarball download (fork: zstandard)
│   └── ...
├── docs/
│   ├── UPDATE_HISTORY_PMA.md        # End-to-end update guide
│   ├── SCHEMAS.md
│   └── ANALYSIS.md
├── src/
│   ├── indexers/polymarket/         # Gamma + Polygon RPC indexers
│   ├── analysis/                    # Upstream analysis scripts
│   └── common/
├── data/
│   ├── polymarket/                  # Raw layers (markets, blocks, trades)
│   └── history_pma/                 # ★ Warehouse output (gitignored)
└── output/                          # Analysis figures
```

---

## Credits

**Raw dataset & research platform:** [Jon Becker](https://github.com/Jon-Becker/prediction-market-analysis) — largest public Polymarket + Kalshi archive.

**Warehouse transformation layer:** [Karlheinz Niebuhr](https://github.com/Karlheinzniebuhr/prediction-market-analysis) — ETL, Windows workflow, Phase 2 hardening.

### Research citations (upstream dataset)

- Becker, J. (2026). _The Microstructure of Wealth Transfer in Prediction Markets_. https://jbecker.dev/research/prediction-market-microstructure
- Le, N. A. (2026). _Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets_. arXiv. https://arxiv.org/abs/2602.19520
- Akey P., et al. (2026). _Who Wins and Who Loses In Prediction Markets?_ SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6443103
- Vedova, J. (2026). _Who Profits from Prediction Markets? Execution, not Information_. SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6191618

---

## Contributing

Issues and PRs welcome. For warehouse/ETL changes, include a smoke command (`--max-trade-files 10`) or note which Phase 1/2 step you tested.

For upstream indexer/analysis bugs, consider opening on [Jon-Becker/prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis/issues) as well.
