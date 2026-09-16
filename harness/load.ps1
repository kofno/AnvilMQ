param(
    [string]$Project = 'anvilmq-harness',
    [ValidateRange(1,128)][int]$Producers = 4,
    [ValidateRange(1,128)][int]$Workers = 4,
    [ValidateRange(1,300)][int]$DurationSeconds = 15,
    [ValidateRange(0,60)][int]$WarmupSeconds = 3,
    [ValidateRange(0,1000000)][int]$PayloadBytes = 1024,
    [ValidateRange(0,30000)][int]$WorkMs = 0,
    [ValidateRange(0,100000)][int]$Rate = 0,
    [ValidateRange(1,600)][int]$DrainSeconds = 60
)
$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
function Compose {
    & docker compose -p $Project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" }
}
Compose build broker runner
Compose up -d --wait broker
Compose run --rm -e "LOAD_PRODUCERS=$Producers" -e "LOAD_WORKERS=$Workers" -e "LOAD_DURATION_SECONDS=$DurationSeconds" -e "LOAD_WARMUP_SECONDS=$WarmupSeconds" -e "LOAD_PAYLOAD_BYTES=$PayloadBytes" -e "LOAD_WORK_MS=$WorkMs" -e "LOAD_RATE=$Rate" -e "LOAD_DRAIN_SECONDS=$DrainSeconds" runner load
