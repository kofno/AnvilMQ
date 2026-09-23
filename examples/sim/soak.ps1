# Run the high-volume soak profile against the local example to measure throughput.
# Brings up (or reconfigures) the producer in `soak` mode pushing toward -MaxJobs at -Rate/s.
# Usage: .\examples\sim\soak.ps1 -MaxJobs 1000000 -Rate 2000 -Concurrency 8
param(
  [long]$MaxJobs = 1000000,
  [int]$Rate = 2000,
  [int]$Concurrency = 8,
  [int]$DurationSeconds = 0
)
$ErrorActionPreference = "Stop"
$compose = @("compose", "-p", "anvilmq-sim", "-f", (Join-Path $PSScriptRoot "compose.yaml"))

$env:SIM_PROFILE = "soak"
$env:SIM_MAX_JOBS = "$MaxJobs"
$env:SIM_RATE = "$Rate"
$env:SIM_DURATION_SECONDS = "$DurationSeconds"
$env:SIM_WORKER_CONCURRENCY = "$Concurrency"

Write-Host "Starting soak: max=$MaxJobs rate=$Rate/s concurrency=$Concurrency" -ForegroundColor Cyan
& docker @compose up -d --build broker
if ($LASTEXITCODE -ne 0) { throw "docker compose up broker failed" }
& docker @compose up -d --build workers
if ($LASTEXITCODE -ne 0) { throw "docker compose up workers failed" }
# Recreate the producer so it picks up the soak env, and follow its progress.
& docker @compose up -d --force-recreate --build producer
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

Write-Host "Following producer (Ctrl+C stops following, not the run):" -ForegroundColor Cyan
& docker @compose logs -f producer
