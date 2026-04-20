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
uv sync
```

Download and extract the pre-collected dataset (36GiB compressed):

```bash
# Linux / macOS
make setup

# Windows / cross-platform
python scripts/download.py
```

This downloads `data.tar.zst` from [Cloudflare R2 Storage](https://s3.jbecker.dev/data.tar.zst) and extracts it to `data/`.

### Data Collection

Collect market and trade data from prediction market APIs:

```bash
# Interactive menu (Linux/macOS — requires simple-term-menu)
make index

# Non-interactive / Windows
python main.py index polymarket_trades
python main.py index polymarket_markets
python main.py index polymarket_blocks
```

Data is saved to `data/kalshi/` and `data/polymarket/` directories. Progress is saved automatically, so you can interrupt and resume collection.

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

The raw data stored under `data/polymarket/` contains on-chain OrderFilled events (trades),
block timestamps, and market metadata — all as Parquet files with raw blockchain field names
and values. This format is excellent for archival and general analysis, but it is not
directly usable for wallet-level behavioral studies.

The `build_history_pma.py` script transforms this raw data into a **research-ready warehouse**
called `history_pma` — a structured, date-partitioned Parquet dataset designed for efficient
querying of "who traded what, when, and in which direction."

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
history_pma/
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
FROM read_parquet('history_pma/warehouse/facts/trade_legs/**/*.parquet', hive_partitioning=true)
WHERE tsSec BETWEEN 1700000000 AND 1710000000
GROUP BY wallet
ORDER BY realized_pnl DESC
LIMIT 20
```

**Polars example** — load all BTC-related trades:

```python
import polars as pl

legs = pl.scan_parquet("history_pma/warehouse/facts/trade_legs/**/*.parquet", hive_partitioning=True)
markets = pl.scan_parquet("history_pma/warehouse/dim/markets.parquet")
btc_conditions = markets.filter(pl.col("question").str.contains("(?i)btc|bitcoin")).select("conditionId")
btc_legs = legs.join(btc_conditions.collect(), on="conditionId").collect()
```

### Building the warehouse

```bash
# Full build from raw data (processes ~40k trade files, ~27 GB output)
python scripts/build_history_pma.py \
  --source-root data/polymarket \
  --output-root data/history_pma

# Test with a small subset first
python scripts/build_history_pma.py \
  --source-root data/polymarket \
  --output-root data/history_pma_test \
  --max-trade-files 10

# Build from external drive (avoids local disk copies)
python scripts/build_history_pma.py \
  --source-root E:/prediction-market-analysis/data/polymarket \
  --output-root E:/history_pma
```

Options:

| Flag | Default | Description |
|------|---------|-------------|
| `--source-root` | (required) | Path to raw PMA data (must contain `markets/`, `trades/`, `blocks/`) |
| `--output-root` | (required) | Output path for the warehouse |
| `--exclude-durations` | `5m,15m` | Updown market durations to exclude |
| `--max-trade-files` | `0` (all) | Limit trade files processed (for testing) |
| `--block-cache-files` | `6` | Max block-timestamp files cached in memory |

## Project Structure

```
├── scripts/
│   ├── build_history_pma.py  # Warehouse builder (Python, cross-platform)
│   ├── download.py           # Cross-platform dataset downloader
│   ├── download.sh           # Linux/macOS dataset downloader
│   └── install-tools.sh      # Linux/macOS tool installer
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
│   └── history_pma/          # Built warehouse (gitignored, generated)
│       ├── state/
│       └── warehouse/
│           ├── dim/
│           └── facts/
├── docs/                     # Documentation
└── output/                   # Analysis outputs (figures, CSVs)
```

## Documentation

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
