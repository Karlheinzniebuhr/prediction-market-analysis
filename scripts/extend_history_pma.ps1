# Phase 2: extend history_pma beyond the hosted snapshot (~Jan 2026).
# Run AFTER Phase 1 (update_history_pma.ps1) or whenever raw trades lag chain head.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
#   powershell ... -File scripts/extend_history_pma.ps1 -FromStep 3   # skip completed indexers
#   powershell ... -File scripts/extend_history_pma.ps1 -SkipIndexers # build only
param(
    [switch]$SkipIndexers,
    [ValidateRange(1, 4)]
    [int]$FromStep = 1
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_pma_invoke.ps1")
$RepoRoot = Split-Path $PSScriptRoot -Parent
$LogDir = Join-Path $RepoRoot "logs"
$SourceRoot = if ($env:HISTORY_PMA_SOURCE_ROOT) { $env:HISTORY_PMA_SOURCE_ROOT } else { "data/polymarket" }
$OutputRoot = if ($env:HISTORY_PMA_OUTPUT_ROOT) { $env:HISTORY_PMA_OUTPUT_ROOT } else { "data/history_pma" }
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
$env:PYTHONUNBUFFERED = "1"

if (-not $SkipIndexers) {
    if ($FromStep -le 1) {
        Invoke-PmaStep "[1/4] Extending markets metadata (Gamma API)..." (Join-Path $LogDir "index_markets_${RunStamp}.log") {
            uv run python -u main.py index polymarket_markets
        }
    }

    if ($FromStep -le 2) {
        Invoke-PmaStep "[2/4] Extending block timestamps (Polygon RPC)..." (Join-Path $LogDir "index_blocks_${RunStamp}.log") {
            uv run python -u main.py index polymarket_blocks
        }
    }

    if ($FromStep -le 3) {
        Invoke-PmaStep "[3/4] Extending raw trades (Polygon RPC) - long-running, resumes from .backfill_block_cursor ..." (Join-Path $LogDir "index_trades_${RunStamp}.log") {
            uv run python -u main.py index polymarket_trades
        }
    }
} else {
    Write-Host "[skip] Indexers skipped (-SkipIndexers)."
}

Invoke-PmaStep '[4/4] Incremental warehouse build (--resume, new trade files only)...' (Join-Path $LogDir "build_history_pma_extend_${RunStamp}.log") {
    uv run python -u scripts/build_history_pma.py `
      --source-root $SourceRoot `
      --output-root $OutputRoot `
      --exclude-durations='' `
      --resume
}

Write-Host ('Done. Check latest date partitions under ' + $OutputRoot + '/warehouse/facts/trade_legs')
