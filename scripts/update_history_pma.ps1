# Phase 1: download hosted PMA raw data and rebuild the history_pma warehouse.
# Coverage caps at ~Jan 2026 (hosted tarball). For current-month data, run extend_history_pma.ps1.
#
# Usage (from repo root):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/update_history_pma.ps1
#
# Optional env overrides:
#   HISTORY_PMA_SOURCE_ROOT  (default: data/polymarket)
#   HISTORY_PMA_OUTPUT_ROOT  (default: data/history_pma)
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

Invoke-PmaStep "[1/2] Downloading hosted raw PMA snapshot..." (Join-Path $LogDir "download_${RunStamp}.log") {
    uv run python -u scripts/download.py --force
}

Invoke-PmaStep "[2/2] Building warehouse at $OutputRoot ..." (Join-Path $LogDir "build_history_pma_${RunStamp}.log") {
    uv run python -u scripts/build_history_pma.py `
      --source-root $SourceRoot `
      --output-root $OutputRoot `
      --exclude-durations=''
}

Write-Host "Done. See $OutputRoot/state/import_progress.json"
