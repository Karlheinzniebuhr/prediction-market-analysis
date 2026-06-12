# Updating the `history_pma` warehouse

This repo builds a **research warehouse** of crypto Polymarket trade legs from raw Parquet
under `data/polymarket/`. The builder writes to `data/history_pma/` by default (override with
`HISTORY_PMA_OUTPUT_ROOT`).

## Two phases (read this first)

| Phase | Goal | Command | Latest trade date |
|-------|------|---------|-------------------|
| **1 — Bulk refresh** | Repair dim tables + replay hosted snapshot | `scripts/update_history_pma.ps1` | **~Jan 25, 2026** (host tarball cap) |
| **2 — Extend to current** | RPC backfill Jan → chain head, append warehouse | `scripts/extend_history_pma.ps1` | **Today** (hours–days of RPC) |

**Phase 1 alone does not bring the warehouse past ~January 2026** — the hosted tarball has
not been republished with newer trades. To reach the current month, run Phase 2 after Phase 1.

## Phase 1 — bulk refresh (hosted snapshot)

From the repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/update_history_pma.ps1
```

Or step by step:

```powershell
uv sync
uv run python -u scripts/download.py --force
# If extract leaves macOS sidecar files, remove before build:
# Get-ChildItem data/polymarket -Recurse -Filter '._*' | Remove-Item -Force
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations=''
```

**Runtime (typical):** download 1–3 hours (bandwidth); full build a few hours over ~40k trade files.

| Step | Input | Output |
|------|--------|--------|
| `scripts/download.py` | `https://s3.jbecker.dev/data.tar.zst` | `data/polymarket/{markets,trades,blocks}/` |
| `scripts/build_history_pma.py` | Raw parquet above | `data/history_pma/warehouse/` + `state/` |

## Phase 2 — extend to current (Gamma + Polygon RPC)

After Phase 1 (or when the latest `date=*` partition still ends around January 2026):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
```

| Step | Indexer | Source | Raw output | Resume cursor |
|------|---------|--------|------------|---------------|
| 1 | `polymarket_markets` | **Gamma API** | `data/polymarket/markets/` | `.backfill_markets_cursor` |
| 2 | `polymarket_blocks` | Polygon RPC | `data/polymarket/blocks/` | highest `blocks_*_*.parquet` end block |
| 3 | `polymarket_trades` | Polygon RPC | `data/polymarket/trades/` | `.backfill_block_cursor` |
| 4 | `build_history_pma.py --resume` | local join | `data/history_pma/warehouse/` | `state/import_progress.json` |

Step 3 is the long pole (~millions of blocks behind chain head after the snapshot). Step 4
skips trade files already counted in `import_progress.json`.

Configure `.env` in the repo root (see `.env.example` if present):

```
POLYGON_RPC=https://polygon.drpc.org
TRADES_CHUNK_SIZE=200
TRADES_MAX_WORKERS=6
TRADES_INFLIGHT_CHUNKS=8
TRADES_BATCH_PAUSE_SEC=0.75
BLOCKS_MAX_WORKERS=8
```

Optional custom warehouse location:

```powershell
$env:HISTORY_PMA_OUTPUT_ROOT = "/path/to/large/drive/history_pma"
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
```

Rebuild only (raw trades already extended):

```powershell
powershell ... -File scripts/extend_history_pma.ps1 -SkipIndexers
```

Resume after a partial Phase 2 run (e.g. markets/blocks done, trades interrupted):

```powershell
powershell ... -File scripts/extend_history_pma.ps1 -FromStep 3
```

**Windows note:** tqdm progress bars write to stderr. The update scripts normalize this so PowerShell does not treat progress output as a fatal error.

## Incremental rebuild (build logic changed, raw files unchanged)

```powershell
uv run python -u scripts/build_history_pma.py `
  --source-root data/polymarket `
  --output-root data/history_pma `
  --exclude-durations='' `
  --resume
```

## Verify

```powershell
Get-Content data/history_pma/state/registry_meta.json
Get-Content data/history_pma/state/import_progress.json
Get-ChildItem data/history_pma/warehouse/facts/trade_legs -Directory |
  Sort-Object Name -Descending | Select-Object -First 3 Name
Get-Content data/polymarket/.backfill_block_cursor
```

- **Phase 1 done:** latest `date=*` ≈ `2026-01-25`, `files_processed` ≈ 40503
- **Phase 2 done:** latest `date=*` near today; `files_processed` increased; cursor near chain head

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `zstandard is required for decompression` | `uv sync` |
| DuckDB `No magic bytes` on `._*.parquet` | Remove `._*` under `data/polymarket` |
| PowerShell `--exclude-durations: expected one argument` | Use `--exclude-durations=''` |
| Warehouse still ends at Jan 2026 after Phase 1 | Expected — run Phase 2 |
| Phase 2 stopped mid-run | Re-run `extend_history_pma.ps1` (indexers resume) |

## Hosted snapshot freshness

```bash
uv run python -c "import httpx; r=httpx.head('https://s3.jbecker.dev/data.tar.zst', follow_redirects=True); print(r.headers.get('last-modified'), r.headers.get('content-length'))"
```

If the host has not published a newer tarball, Phase 1 refreshes dims and replays the same
trade coverage through roughly **Jan 2026**.
