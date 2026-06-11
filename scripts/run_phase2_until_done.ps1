# Phase 2 watchdog: resume trades indexer until chain head, then incremental warehouse build.
# Survives transient RPC failures by retrying until complete.
param(
    [string]$SourceRoot = $(if ($env:HISTORY_PMA_SOURCE_ROOT) { $env:HISTORY_PMA_SOURCE_ROOT } else { "data/polymarket" }),
    [string]$OutputRoot = $(if ($env:HISTORY_PMA_OUTPUT_ROOT) { $env:HISTORY_PMA_OUTPUT_ROOT } else { "data/history_pma" })
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "_pma_invoke.ps1")
$RepoRoot = Split-Path $PSScriptRoot -Parent
$LogDir = Join-Path $RepoRoot "logs"
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$CursorFile = Join-Path $RepoRoot "data/polymarket/.backfill_block_cursor"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
$env:PYTHONUNBUFFERED = "1"

$pass = 0
while (Test-Path $CursorFile) {
    $pass += 1
    $cursor = Get-Content $CursorFile -Raw
    Write-Host "[trades pass $pass] resuming from block $cursor"
    Invoke-PmaStep "[trades] Polygon RPC backfill (pass $pass)" (Join-Path $LogDir "index_trades_watchdog_${RunStamp}_pass${pass}.log") {
        uv run python -u main.py index polymarket_trades
    }
    if (Test-Path $CursorFile) {
        Write-Host "[trades] pass $pass ended early; retrying in 30s ..."
        Start-Sleep -Seconds 30
    }
}

Write-Host "[build] trades complete; running incremental warehouse build"
Invoke-PmaStep "[build] history_pma --resume" (Join-Path $LogDir "build_history_pma_watchdog_${RunStamp}.log") {
    uv run python -u scripts/build_history_pma.py `
      --source-root $SourceRoot `
      --output-root $OutputRoot `
      --exclude-durations='' `
      --resume
}

Write-Host "Phase 2 complete. Warehouse: $OutputRoot"
