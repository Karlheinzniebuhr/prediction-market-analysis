# prediction-market-analysis

Fork of [Jon Becker's prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) with an added **warehouse ETL** that turns raw Polymarket on-chain fills into ML-ready trade legs. Becker's repo ships the largest public Polymarket archive — hosted Parquet of markets, blocks, and `OrderFilled` events, plus indexers and analysis scripts. This fork adds the missing layer between that archive and wallet-level research: timestamps, market linkage, explicit buy/sell direction, and Hive-partitioned output you can query in DuckDB or Polars without writing join-and-decode glue code every time.

Pure Python. Cross-platform (Windows, macOS, Linux). No Node.js.

---

## The problem

Raw Polymarket blockchain data answers *what happened on-chain* but not *who traded what, when, and in which direction* in a form models can consume.

Each fill arrives with opaque 77-digit token IDs, a block number instead of a clock time, and two asset legs where one side is USDC collateral and the other is an outcome token. There is no `buy` or `sell` column. Linking a fill to a human-readable market means joining Gamma metadata. Resolving time means joining block timestamp files. Splitting maker and taker into separate attributable rows means understanding CTF exchange semantics.

A question as simple as *"what did wallet X trade on March 15?"* touches four tables and a collateral decode before you can aggregate. Scale that to rolling per-wallet features for ML — windows recomputed at every timestep, no look-ahead — and the preprocessing cost dominates the research.

---

## What this fork produces

`scripts/build_history_pma.py` transforms Becker's raw layers into **`history_pma`**: a Snappy-compressed Parquet warehouse (~27 GB at snapshot scale) of **crypto-market trade legs**, partitioned by UTC date.

| Input (raw) | Output (warehouse) |
|---|---|
| Block numbers | `tsSec` — real Unix timestamps |
| 77-digit token IDs | `tokenId`, `conditionId`, `outcomeIndex` via dim tables |
| One row per on-chain fill | **Two legs** per fill (maker + taker) |
| Implicit asset legs | Explicit `side`: `buy` or `sell` |
| All Polymarket categories | Pre-filtered **crypto markets** |
| Monolithic trade files | **`date=YYYY-MM-DD/`** Hive partitions |

At snapshot scale the pipeline processes **185M+ raw fill events** into timestamped, wallet-attributed, direction-labeled legs across **130K+ tokens** and **150K+ markets**. Partition pruning lets DuckDB and Polars skip irrelevant dates — queries that would run for hours on raw joins finish in seconds.

---

## How it relates to upstream

```
Jon-Becker/prediction-market-analysis          This fork
─────────────────────────────────────        ─────────────────────────────
Hosted tarball (data.tar.zst)          →     same download + resumable extract
polymarket_markets / blocks / trades   →     same indexers, hardened for Phase 2
Kalshi indexers + analysis scripts     →     inherited unchanged
                                         +     build_history_pma.py
                                         +     Phase 1/2 update scripts (Windows)
                                         +     RPC probe + trades indexer tuning
```

Upstream owns the raw archive and research figures. **This fork owns the transformation into `history_pma`.**

---

## Pipeline

Three raw Parquet layers (from Becker's dataset or live indexers) feed one builder:

```
data/polymarket/markets/     Gamma API metadata
data/polymarket/blocks/      block_number → timestamp
data/polymarket/trades/      CTF + NegRisk OrderFilled events
              │
              ▼
     build_history_pma.py
              │
              ▼
data/history_pma/warehouse/
├── dim/markets.parquet
├── dim/tokens.parquet
└── facts/trade_legs/date=YYYY-MM-DD/part-*.parquet
```

The builder runs six steps:

1. **Registry** — DuckDB scan of market Parquet; filter to crypto via keyword match; emit `dim/markets` and `dim/tokens`; map every outcome token ID to `(conditionId, outcomeIndex, outcome)`.
2. **Block resolution** — binary search across ~785 block-range files; LRU cache (`--block-cache-files`, default 6) converts `block_number` → `tsSec`.
3. **Leg expansion** — each fill becomes maker and taker rows with `wallet`, `counterparty`, `role`, `shares`, `usdc`, `price`, `feeUsdc`.
4. **Direction decode** — whichever leg is collateral (`asset_id == 0`) determines buy vs sell for each side.
5. **Partition write** — legs land in `date=YYYY-MM-DD/` from resolved timestamp.
6. **Resume** — `state/import_progress.json` tracks processed trade files; `--resume` skips them on incremental append.

---

## Warehouse layout and schema

```
data/history_pma/
├── state/
│   ├── registry_meta.json       # market/token counts, filters applied
│   └── import_progress.json     # files processed, legs written
└── warehouse/
    ├── dim/markets.parquet
    ├── dim/tokens.parquet
    └── facts/trade_legs/date=YYYY-MM-DD/part-*.parquet
```

Each trade leg:

| Column | Meaning |
|--------|---------|
| `fillId` | Stable id: `txHash_logIndex_contract` |
| `tsSec` | Unix timestamp |
| `wallet` / `counterparty` | On-chain addresses |
| `role` | `maker` or `taker` |
| `side` | `buy` or `sell` |
| `tokenId`, `conditionId`, `outcomeIndex` | Market linkage |
| `shares`, `usdc`, `price`, `feeUsdc` | Decimal amounts |

Raw Parquet schemas: [docs/SCHEMAS.md](docs/SCHEMAS.md).

---

## Installation

Python 3.9+, [uv](https://github.com/astral-sh/uv):

```bash
git clone https://github.com/Karlheinzniebuhr/prediction-market-analysis
cd prediction-market-analysis
uv sync
```

Optional `.env` for Phase 2 Polygon RPC (see [Quick start](#quick-start)).

Point output at a large drive when needed:

```powershell
$env:HISTORY_PMA_SOURCE_ROOT = "path/to/data/polymarket"
$env:HISTORY_PMA_OUTPUT_ROOT  = "path/to/history_pma"
```

---

## Quick start

Updating the warehouse is two phases. **Phase 1 alone stops at ~January 2026** — the hosted tarball has not been republished with newer trades. Phase 2 extends raw data to chain head via RPC, then appends warehouse partitions.

### Phase 1 — hosted snapshot + full build

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/update_history_pma.ps1
```

Or step by step:

```powershell
uv run python -u scripts/download.py --force
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations=''
```

Downloads [data.tar.zst](https://s3.jbecker.dev/data.tar.zst) (~36 GiB), extracts with `zstandard`, rebuilds the full warehouse.

### Phase 2 — extend to chain head

Run all four steps:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
```

Or run indexers manually. **Each step resumes** from its cursor file after interrupt or crash.

```powershell
$env:PYTHONUNBUFFERED = "1"

uv run python -u main.py index polymarket_markets   # Gamma → data/polymarket/markets/
uv run python -u main.py index polymarket_blocks    # Polygon → data/polymarket/blocks/
uv run python -u main.py index polymarket_trades    # Polygon → data/polymarket/trades/
                                                    # cursor: .backfill_block_cursor

uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations='' `
  --resume                                          # skips snapshot files already imported
```

Step 3 (trades) is the long pole — days on free RPC. Tune concurrency with `scripts/probe_trades_rpc.py`. Example `.env`:

```env
POLYGON_RPC=https://polygon.drpc.org
TRADES_CHUNK_SIZE=100
TRADES_MAX_WORKERS=20
TRADES_INFLIGHT_CHUNKS=20
```

Skip indexers if raw data is already current:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1 -SkipIndexers
```

Resume mid-Phase-2 if markets and blocks are done:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1 -FromStep 3
```

### Verify

```powershell
Get-Content data/history_pma/state/import_progress.json
Get-Content data/history_pma/state/registry_meta.json
Get-ChildItem data/history_pma/warehouse/facts/trade_legs -Directory |
  Sort-Object Name -Descending | Select-Object -First 3 Name
```

Phase 2 trades complete when `.backfill_block_cursor` is deleted. Full checklist: [docs/UPDATE_HISTORY_PMA.md](docs/UPDATE_HISTORY_PMA.md).

---

## Querying the warehouse

**DuckDB** — net flow per wallet over a time window:

```sql
SELECT wallet,
       SUM(CASE WHEN side = 'buy' THEN -usdc ELSE usdc END) AS net_flow
FROM read_parquet('data/history_pma/warehouse/facts/trade_legs/**/*.parquet', hive_partitioning=true)
WHERE tsSec BETWEEN 1700000000 AND 1710000000
GROUP BY wallet
ORDER BY net_flow DESC
LIMIT 20;
```

**Polars** — BTC-related legs joined to market questions:

```python
import polars as pl

legs = pl.scan_parquet("data/history_pma/warehouse/facts/trade_legs/**/*.parquet", hive_partitioning=True)
markets = pl.scan_parquet("data/history_pma/warehouse/dim/markets.parquet")
btc = markets.filter(pl.col("question").str.contains("(?i)btc|bitcoin")).select("conditionId")
df = legs.join(btc.collect(), on="conditionId").collect()
```

---

## Fork additions beyond upstream

| Component | Role |
|-----------|------|
| `scripts/build_history_pma.py` | Warehouse ETL |
| `scripts/update_history_pma.ps1` | Phase 1 orchestration |
| `scripts/extend_history_pma.ps1` | Phase 2 orchestration |
| `scripts/download.py` | Resumable tarball download + zstd extract |
| `scripts/probe_trades_rpc.py` | RPC chunk/concurrency tuning |
| `scripts/run_phase2_until_done.ps1` | Watchdog runner |
| `src/indexers/polymarket/trades.py` | Continuous pipeline, split-on-oversize, cursor+1 resume, live stderr progress |

### Inherited from upstream

- Kalshi and Polymarket raw indexers (markets, blocks, trades)
- Analysis framework: `uv run python main.py analyze <name>`
- Consolidated `HttpClient` with rate limiting and retries
- Hosted dataset and research scripts

Analysis docs: [docs/ANALYSIS.md](docs/ANALYSIS.md).

---

## Project layout

```
scripts/
  build_history_pma.py      warehouse ETL
  update_history_pma.ps1    Phase 1
  extend_history_pma.ps1      Phase 2
  probe_trades_rpc.py         RPC tuning
  download.py                 tarball download
src/
  indexers/polymarket/        Gamma + Polygon indexers
  analysis/                   upstream research scripts
docs/
  UPDATE_HISTORY_PMA.md       full update guide
  SCHEMAS.md
data/
  polymarket/                 raw input (gitignored)
  history_pma/                warehouse output (gitignored)
```

---

## Credits and citations

**Raw dataset and research platform:** [Jon Becker](https://github.com/Jon-Becker/prediction-market-analysis).

**Warehouse ETL and Phase 2 tooling:** [Karlheinz Niebuhr](https://x.com/karlbooklover).

Research using the underlying dataset:

- Becker, J. (2026). _The Microstructure of Wealth Transfer in Prediction Markets_. https://jbecker.dev/research/prediction-market-microstructure
- Le, N. A. (2026). _Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets_. arXiv. https://arxiv.org/abs/2602.19520
- Akey P., et al. (2026). _Who Wins and Who Loses In Prediction Markets?_ SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6443103
- Vedova, J. (2026). _Who Profits from Prediction Markets? Execution, not Information_. SSRN. https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6191618

---

## Contributing

PRs welcome. For warehouse changes, note which Phase you tested or run a smoke build with `--max-trade-files 10`. Upstream indexer bugs may also belong on [Jon-Becker/prediction-market-analysis/issues](https://github.com/Jon-Becker/prediction-market-analysis/issues).
