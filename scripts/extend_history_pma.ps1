# Phase 2: extend history_pma beyond the hosted snapshot (~Jan 2026).
# Run AFTER Phase 1 (update_history_pma.ps1) or whenever raw trades lag chain head.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/extend_history_pma.ps1
#   powershell ... -File scripts/extend_history_pma.ps1 -SkipIndexers
param(
    [switch]$SkipIndexers
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path $PSScriptRoot -Parent
$LogDir = Join-Path $RepoRoot "logs"
$SourceRoot = if ($env:HISTORY_PMA_SOURCE_ROOT) { $env:HISTORY_PMA_SOURCE_ROOT } else { "data/polymarket" }
$OutputRoot = if ($env:HISTORY_PMA_OUTPUT_ROOT) { $env:HISTORY_PMA_OUTPUT_ROOT } else { "data/history_pma" }
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
$env:PYTHONUNBUFFERED = "1"

function Invoke-PmaStep {
    param(
        [string]$Label,
        [string]$LogName,
        [scriptblock]$Command
    )
    $logPath = Join-Path $LogDir "${LogName}_${RunStamp}.log"
    Write-Host $Label
    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Command 2>&1 | Tee-Object -FilePath $logPath
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prevEap
    if ($code -ne 0) {
        Write-Error "$LogName failed with exit code $code (log: $logPath)"
    }
}

if (-not $SkipIndexers) {
    Invoke-PmaStep "[1/4] Extending markets metadata (Gamma API)..." "index_markets" {
        uv run python -u main.py index polymarket_markets
    }

    Invoke-PmaStep "[2/4] Extending block timestamps (Polygon RPC)..." "index_blocks" {
        uv run python -u main.py index polymarket_blocks
    }

    Invoke-PmaStep "[3/4] Extending raw trades (Polygon RPC) - long-running, resumes from .backfill_block_cursor ..." "index_trades" {
        uv run python -u main.py index polymarket_trades
    }
} else {
    Write-Host "[skip] Indexers skipped (-SkipIndexers)."
}

Invoke-PmaStep '[4/4] Incremental warehouse build (--resume, new trade files only)...' "build_history_pma_extend" {
    uv run python -u scripts/build_history_pma.py `
      --source-root $SourceRoot `
      --output-root $OutputRoot `
      --exclude-durations='' `
      --resume
}

Write-Host ('Done. Check latest date partitions under ' + $OutputRoot + '/warehouse/facts/trade_legs')
