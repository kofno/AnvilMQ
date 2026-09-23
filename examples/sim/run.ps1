# Build and start the local AnvilMQ example, then wait for the broker.
# Usage from the repository root: .\examples\sim\run.ps1
# Starts a steady synthetic ~13 jobs/s mixed workload (prod profile).
$ErrorActionPreference = "Stop"
$compose = @("compose", "-p", "anvilmq-sim", "-f", (Join-Path $PSScriptRoot "compose.yaml"))

Write-Host "Building and starting anvilmq-sim (first broker build compiles Rust in release; be patient)..." -ForegroundColor Cyan
& docker @compose up -d --build
if ($LASTEXITCODE -ne 0) { throw "docker compose up failed" }

Write-Host "Waiting for broker /readyz on http://127.0.0.1:9092 ..." -ForegroundColor Cyan
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
  try {
    $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 "http://127.0.0.1:9092/readyz"
    if ($r.StatusCode -eq 200) { $ready = $true; break }
  } catch { Start-Sleep -Seconds 2 }
}
if (-not $ready) { throw "broker did not become ready; check: docker @compose logs broker" }

Write-Host ""
Write-Host "anvilmq-sim is up." -ForegroundColor Green
Write-Host "  Grafana:    http://127.0.0.1:3000  (AnvilMQ queue health; admin/admin, demo-only)"
Write-Host "  Prometheus: http://127.0.0.1:9490"
Write-Host "  Broker:     grpc 127.0.0.1:50071, metrics http://127.0.0.1:9092/metrics"
Write-Host "  Loki:       http://127.0.0.1:3100"
Write-Host ""
Write-Host "  From examples\sim:"
Write-Host "  Logs:       docker compose logs -f workers producer"
Write-Host "  Semantics:  docker compose run --rm workers /app/examples/sim/src/semantics-check.ts"
Write-Host "  Search:     docker compose run --rm workers /app/examples/sim/src/search-demo.ts"
Write-Host "  Soak:       .\soak.ps1 -MaxJobs 1000000 -Rate 2000"
Write-Host "  Stop:       docker compose down -v"
