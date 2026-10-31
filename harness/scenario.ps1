param(
    [string]$Project = 'anvilmq-harness',
    [ValidateRange(1,500)][int]$Schedulers = 50,
    [ValidateRange(1,15)][int]$Minutes = 3,
    [ValidateRange(1,128)][int]$Workers = 8,
    [ValidateRange(1,1000)][int]$BulkBatches = 5,
    [ValidateRange(1,5000)][int]$BulkSize = 2000,
    [ValidateRange(0,60000)][int]$BulkIntervalMs = 1000,
    [ValidateRange(0,1000000)][int]$PayloadBytes = 256,
    [ValidateRange(1,600)][int]$DrainSeconds = 120,
    [ValidateRange(1,1000)][int]$PollMs = 10
)
$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
function Compose {
    & docker compose -p $Project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" }
}
Compose build broker runner
Compose up -d --wait broker
Compose run --rm `
    -e "SCENARIO_SCHEDULERS=$Schedulers" `
    -e "SCENARIO_MINUTES=$Minutes" `
    -e "SCENARIO_WORKERS=$Workers" `
    -e "SCENARIO_BULK_BATCHES=$BulkBatches" `
    -e "SCENARIO_BULK_SIZE=$BulkSize" `
    -e "SCENARIO_BULK_INTERVAL_MS=$BulkIntervalMs" `
    -e "SCENARIO_PAYLOAD_BYTES=$PayloadBytes" `
    -e "SCENARIO_DRAIN_SECONDS=$DrainSeconds" `
    -e "SCENARIO_POLL_MS=$PollMs" `
    runner scenario
