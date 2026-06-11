# Prediction Market Analysis

A framework for analyzing prediction market data, including the largest publicly available dataset of Polymarket and Kalshi market and trade data. Provides tools for data collection, storage, and running analysis scripts that generate figures and statistics.

## Overview

This project enables research and analysis of prediction markets by providing:
- Pre-collected datasets from Polymarket and Kalshi
- Data collection indexers for gathering new data
- Analysis framework for generating figures and statistics

Currently supported features:
- Market metadata collection (Kalshi & Polymarket)
- Trade history collection via API and blockchain
- Parquet-based storage with automatic progress saving
- Extensible analysis script framework

## Installation & Usage

Requires Python 3.9+. Install dependencies with [uv](https://github.com/astral-sh/uv):

```bash
uv sync   # httpx, zstandard, duckdb, pyarrow, web3, ...
```

Download and extract the pre-collected dataset (36GiB compressed; requires `zstandard`):

```bash
# Linux / macOS
make setup

# Windows / cross-platform
python scripts/download.py
```

This downloads `data.tar.zst` from [Cloudflare R2 Storage](https://s3.jbecker.dev/data.tar.zst) and extracts it to `data/`.

### Data Collection

Collect market and trade data from prediction market APIs and Polygon RPC:

```bash
# Interactive menu (Linux/macOS — requires simple-term-menu)
make index

# Non-interactive / Windows — three Polymarket indexers (run in this order for extensions)
python main.py index polymarket_markets   # Gamma API metadata → data/polymarket/markets/
python main.py index polymarket_blocks    # Polygon block timestamps → data/polymarket/blocks/
python main.py index polymarket_trades    # Polygon OrderFilled events → data/polymarket/trades/
```

Data is saved to `data/kalshi/` and `data/polymarket/`. Progress is saved automatically (cursor files under `data/polymarket/`), so you can interrupt and resume.

> These indexers are **Phase 2** of the warehouse update (extend raw data beyond the hosted snapshot). For the full workflow, see [Building the history_pma Warehouse](#building-the-history_pma-warehouse) below.

### Running Analyses

```bash
# Interactive menu
make analyze

# Non-interactive / Windows
python main.py analyze all
python main.py analyze <analysis_name>
```

Output files (PNG, PDF, CSV, JSON) are saved to `output/`.

### Packaging Data

To compress the data directory for storage/distribution:

```bash
make package
```

This creates a zstd-compressed tar archive (`data.tar.zst`) and removes the `data/` directory.

---

## Building the `history_pma` Warehouse

Default output: `data/history_pma/` (gitignored). Override with `HISTORY_PMA_OUTPUT_ROOT` for a
large external drive. Raw source: `data/polymarket/`.

Raw `data/polymarket/` holds **three Parquet layers** that `build_history_pma.py` joins:

| Layer | Indexer | Source | Output |
|-------|---------|--------|--------|
| Markets | `polymarket_markets` | **Gamma API** (`gamma-api.polymarket.com`) | `data/polymarket/markets/*.parquet` |
| Blocks | `polymarket_blocks` | **Polygon RPC** (block timestamps) | `data/polymarket/blocks/*.parquet` |
| Trades | `polymarket_trades` | **Polygon RPC** (CTF/NegRisk `OrderFilled` logs) | `data/polymarket/trades/*.parquet` |

`build_history_pma.py` transforms those into **date-partitioned trade legs** at
`data/history_pma/warehouse/facts/trade_legs/date=YYYY-MM-DD/`.

### End-to-end update (read this first)

**Two phases.** Phase 1 alone does **not** bring the warehouse past ~Jan 2026 — the hosted
tarball has not been republished with newer trades.

| Phase | When to run | One command (Windows) | Resulting max `date=*` |
|-------|-------------|----------------------|------------------------|
| **1 — Bulk refresh** | Repair dims / replay hosted snapshot | `scripts/update_history_pma.ps1` | **~Jan 25, 2026** |
| **2 — Extend to current** | After Phase 1, or when legs lag today | `scripts/extend_history_pma.ps1` | **Chain head** (hours–days RPC) |

```
Phase 1:  download.py  →  raw parquet (snapshot)  →  build_history_pma.py  →  data/history_pma
Phase 2:  polymarket_markets (Gamma)
       →  polymarket_blocks (Polygon)
       →  polymarket_trades (Polygon, resumes .backfill_block_cursor)
       →  build_history_pma.py --resume  →  append new date partitions
```

Full checklist: [docs/UPDATE_HISTORY_PMA.md](docs/UPDATE_HISTORY_PMA.md).

#### Phase 1 — bulk refresh (hosted snapshot)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/update_history_pma.ps1
```

Manual equivalent:

```powershell
uv sync   # includes zstandard (required for download.py extract)
uv run python -u scripts/download.py --force
# If extract leaves macOS sidecar files, remove before build:
# Get-ChildItem data/polymarket -Recurse -Filter '._*' | Remove-Item -Force
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations=''
```

#### Phase 2 — extend to current month (Gamma + Polygon RPC)

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
```

Runs all four steps: Gamma markets → block timestamps → raw trades (long pole) →
`build_history_pma.py --resume` (skips snapshot trade files already in `import_progress.json`).

Configure RPC in `.env`: `POLYGON_RPC`, `TRADES_CHUNK_SIZE=1000`, `TRADES_MAX_WORKERS=16`.

Rebuild only (raw trades already extended):

```powershell
powershell ... -File scripts/extend_history_pma.ps1 -SkipIndexers
```

#### Verify

```powershell
Get-Content data/history_pma/state/registry_meta.json
Get-Content data/history_pma/state/import_progress.json
Get-ChildItem data/history_pma/warehouse/facts/trade_legs -Directory |
  Sort-Object Name -Descending | Select-Object -First 3 Name
```

### Why this transformation exists

The raw PMA trades are **fill-level records** with opaque `maker_asset_id` / `taker_asset_id`
fields (77-digit token IDs) and `block_number` instead of timestamps. To answer questions like
"did wallet X buy or sell Bitcoin-related exposure on March 15?" you need to:

1. **Resolve block numbers to timestamps** — join trades against `blocks/*.parquet` to get
   real dates.
2. **Map asset IDs to markets** — join against `markets/*.parquet` to link token IDs to
   specific prediction markets (condition IDs, questions, outcomes).
3. **Determine trade direction** — decode which side of the fill is USDC (collateral) and
   which is outcome tokens, to derive an explicit `buy` / `sell` side.
4. **Filter to crypto markets** — the raw dataset covers all Polymarket categories; the
   warehouse pre-filters to crypto-related markets via keyword matching.
5. **Split fills into legs** — each fill produces two trade legs (maker + taker), each with
   an explicit `wallet`, `side`, `shares`, `usdc`, `price`, and `feeUsdc`.
6. **Date-partition for efficient access** — the output is partitioned as
   `facts/trade_legs/date=YYYY-MM-DD/*.parquet`, enabling efficient time-range queries with
   DuckDB, Polars, or any Parquet-aware tool.

### What the warehouse contains

```
data/history_pma/
├── state/
│   ├── checkpoint.json          # Processing resume state
│   ├── registry_meta.json       # Registry version, market/token counts, hashes
│   └── import_progress.json     # Files processed, total legs written
└── warehouse/
    ├── dim/
    │   ├── markets.parquet      # Market dimension: conditionId, slug, question, endDate, category
    │   └── tokens.parquet       # Token dimension: tokenId → conditionId, outcomeIndex, outcome
    └── facts/
        └── trade_legs/          # Date-partitioned trade legs
            ├── date=2023-03-05/
            │   ├── part-00000001.parquet
            │   └── ...
            ├── date=2023-03-06/
            └── ...
```

### Trade leg schema

| Column | Type | Description |
|--------|------|-------------|
| `fillId` | string | Unique fill identifier (`txHash_logIndex_contract`) |
| `txHash` | string | Transaction hash |
| `orderHash` | string | Order hash from the OrderFilled event |
| `tsSec` | int64 | Unix timestamp (seconds) — resolved from block number |
| `wallet` | string | Wallet address for this leg (maker or taker) |
| `counterparty` | string | The other side of the trade |
| `role` | string | `"maker"` or `"taker"` |
| `tokenId` | string | Outcome token ID |
| `conditionId` | string | Market condition ID (links to `dim/markets.parquet`) |
| `outcomeIndex` | int32 | 0 = first outcome (typically Yes), 1 = second (typically No) |
| `side` | string | `"buy"` or `"sell"` — derived from which asset is collateral |
| `sharesRaw` | string | Raw share amount (6-decimal integer as string) |
| `usdcRaw` | string | Raw USDC amount (6-decimal integer as string) |
| `feeRaw` | string | Raw fee (only present for maker leg) |
| `shares` | float64 | Shares in human units (sharesRaw / 1e6) |
| `usdc` | float64 | USDC in human units (usdcRaw / 1e6) |
| `feeUsdc` | float64 | Fee in USDC (feeRaw / 1e6, taker leg = 0) |
| `price` | float64 | Effective price (usdc / shares) |
| `registryVersion` | int32 | Schema version for the market registry |
| `source` | string | Provenance label |

### Market dimension schema

| Column | Type | Description |
|--------|------|-------------|
| `conditionId` | string | Unique market identifier |
| `slug` | string | URL-friendly market name |
| `question` | string | Human-readable market question |
| `endDateIso` | string | Market end date (ISO format, nullable) |
| `active` | bool | Whether the market is currently active |
| `closed` | bool | Whether the market has been resolved |
| `category` | string | Always `"crypto"` (pre-filtered) |
| `isExcludedShortTerm` | bool | True for excluded short-term updown markets |
| `registryVersion` | int32 | Schema version |

### How to use it

The warehouse format is designed for DuckDB, Polars, and PyArrow. Thanks to Hive-style
date partitioning, time-range queries skip irrelevant partitions automatically.

**DuckDB example** — compute cumulative PnL per wallet:

```sql
SELECT wallet, SUM(CASE WHEN side='buy' THEN -usdc ELSE usdc END) AS realized_pnl
FROM read_parquet('data/history_pma/warehouse/facts/trade_legs/**/*.parquet', hive_partitioning=true)
WHERE tsSec BETWEEN 1700000000 AND 1710000000
GROUP BY wallet
ORDER BY realized_pnl DESC
LIMIT 20
```

**Polars example** — load all BTC-related trades:

```python
import polars as pl

legs = pl.scan_parquet("data/history_pma/warehouse/facts/trade_legs/**/*.parquet", hive_partitioning=True)
markets = pl.scan_parquet("data/history_pma/warehouse/dim/markets.parquet")
btc_conditions = markets.filter(pl.col("question").str.contains("(?i)btc|bitcoin")).select("conditionId")
btc_legs = legs.join(btc_conditions.collect(), on="conditionId").collect()
```

### `build_history_pma.py` reference

```powershell
# Full rebuild (Phase 1, after download)
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations=''

# Incremental append (Phase 2, after new raw trade files exist)
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations='' `
  --resume
```

```bash
# Linux/macOS
uv run python -u scripts/build_history_pma.py \
  --source-root data/polymarket \
  --output-root data/history_pma \
  --exclude-durations ""
```

Local smoke test only (not production):

```bash
uv run python scripts/build_history_pma.py \
  --source-root data/polymarket \
  --output-root data/history_pma_test \
  --max-trade-files 10
```

| Flag | Default | Description |
|------|---------|-------------|
| `--source-root` | (required) | Raw PMA root with `markets/`, `trades/`, `blocks/` subdirs |
| `--output-root` | (required) | Warehouse root (default in scripts: `data/history_pma`) |
| `--exclude-durations` | `""` | Comma-separated updown durations to drop; empty = include all |
| `--max-trade-files` | `0` (all) | Limit trade files (smoke tests) |
| `--block-cache-files` | `6` | Max block parquet files cached in memory |
| `--resume` | off | Skip files counted in `state/import_progress.json` (Phase 2 append) |

**PowerShell note:** pass an empty exclude list as `--exclude-durations=''`. Bare `""` is swallowed by the shell.

## Project Structure

```
├── scripts/
│   ├── build_history_pma.py         # Raw parquet → trade_legs warehouse
│   ├── update_history_pma.ps1       # Phase 1: download + full build
│   ├── extend_history_pma.ps1       # Phase 2: indexers + --resume append
│   ├── download.py                  # Hosted snapshot download + zstd extract
│   ├── download.sh                  # Linux/macOS dataset downloader
│   └── install-tools.sh             # Linux/macOS tool installer
├── src/
│   ├── analysis/             # Analysis scripts
│   │   ├── kalshi/           # Kalshi-specific analyses
│   │   ├── polymarket/       # Polymarket-specific analyses
│   │   └── comparison/       # Cross-platform comparisons
│   ├── indexers/             # Data collection indexers
│   │   ├── kalshi/           # Kalshi API client and indexers
│   │   └── polymarket/       # Polymarket API/blockchain indexers
│   └── common/               # Shared utilities and interfaces
├── data/                     # Data directory (extracted from data.tar.zst)
│   ├── kalshi/
│   │   ├── markets/
│   │   └── trades/
│   ├── polymarket/
│   │   ├── blocks/           # Block number → timestamp mapping
│   │   ├── markets/          # Market metadata (conditions, outcomes, slugs)
│   │   └── trades/           # Raw CTF/NegRisk OrderFilled events
│   └── .download_complete    # Sentinel after successful download.py extract
├── logs/                     # Update logs (download, indexers, build; gitignored)
├── docs/                     # Documentation
└── output/                   # Analysis outputs (figures, CSVs)
```

## Documentation

- [Updating history_pma](docs/UPDATE_HISTORY_PMA.md) - End-to-end warehouse update (Phase 1 + Phase 2)
- [Data Schemas](docs/SCHEMAS.md) - Parquet file schemas for markets and trades
- [Writing Analyses](docs/ANALYSIS.md) - Guide for writing custom analysis scripts

## Contributing

If you'd like to contribute to this project, please open a pull-request with your changes, as well as detailed information on what is changed, added, or improved.

For more information, see the [contributing guide](CONTRIBUTING.md).

## Issues

If you've found an issue or have a question, please open an issue [here](https://github.com/jon-becker/prediction-market-analysis/issues).

## Research & Citations

- Becker, J. (2026). _The Microstructure of Wealth Transfer in Prediction Markets_. Jbecker. https://jbecker.dev/research/prediction-market-microstructure
- Le, N. A. (2026). _Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets_. arXiv. https://arxiv.org/abs/2602.19520
- Akey P., Gregoire, V., Harvie, N., Martineau, C. (2026). _Who Wins and Who Loses In Prediction Markets? Evidence from Polymarket_. SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6443103
- Vedova, J. (2026). _Who Profits from Prediction Markets? Execution, not Information_. SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6191618

If you have used or plan to use this dataset in your research, please reach out via [email](mailto:jonathan@jbecker.dev) or [Twitter](https://x.com/BeckerrJon) -- i'd love to hear about what you're using the data for! Additionally, feel free to open a PR and update this section with a link to your paper.
