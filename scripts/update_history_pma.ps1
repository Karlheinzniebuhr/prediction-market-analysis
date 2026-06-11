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
$RepoRoot = Split-Path $PSScriptRoot -Parent
$LogDir = Join-Path $RepoRoot "logs"
$SourceRoot = if ($env:HISTORY_PMA_SOURCE_ROOT) { $env:HISTORY_PMA_SOURCE_ROOT } else { "data/polymarket" }
$OutputRoot = if ($env:HISTORY_PMA_OUTPUT_ROOT) { $env:HISTORY_PMA_OUTPUT_ROOT } else { "data/history_pma" }

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $RepoRoot
$env:PYTHONUNBUFFERED = "1"

Write-Host "[1/2] Downloading hosted raw PMA snapshot..."
uv run python -u scripts/download.py --force 2>&1 | Tee-Object -FilePath (Join-Path $LogDir "download.log")
if ($LASTEXITCODE -ne 0) {
    Write-Error "download.py failed with exit code $LASTEXITCODE"
}

Write-Host "[2/2] Building warehouse at $OutputRoot ..."
uv run python -u scripts/build_history_pma.py `
  --source-root $SourceRoot `
  --output-root $OutputRoot `
  --exclude-durations='' `
  2>&1 | Tee-Object -FilePath (Join-Path $LogDir "build_history_pma.log")
if ($LASTEXITCODE -ne 0) {
    Write-Error "build_history_pma.py failed with exit code $LASTEXITCODE"
}

Write-Host "Done. See $OutputRoot/state/import_progress.json"
