param(
    [ValidateRange(1,300)][int]$DurationSeconds = 60,
    [ValidateRange(1,100000)][int]$Rate = 400
)
$ErrorActionPreference = 'Stop'
$compose = Join-Path $PSScriptRoot 'compose.yaml'
$runId = 'durability-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$output = Join-Path $PSScriptRoot "artifacts/$runId"
New-Item -ItemType Directory -Path $output | Out-Null
$priorMode = $env:ANVILMQ_DURABILITY
$priorFile = $env:ANVILMQ_HARNESS_DB_FILE
function Compose {
    & docker compose -p anvilmq-harness -f $compose @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" }
}
Compose build broker runner
& docker info --format '{{json .}}' | Set-Content (Join-Path $output 'docker-info.json')
try {
    $index = 0
    # Reverse the second pair to expose ordering effects. Each run starts with an empty DB.
    foreach ($mode in 'NORMAL','FULL','FULL','NORMAL') {
        $index++
        $env:ANVILMQ_DURABILITY = $mode
        $env:ANVILMQ_HARNESS_DB_FILE = "$runId-$index.db"
        Compose up -d --wait broker
        $logs = Compose logs --no-log-prefix broker
        $logs | Set-Content (Join-Path $output "$index-$mode-broker.log")
        $settings = $logs | Where-Object { $_ -match 'database durability configured' } | ForEach-Object { ($_ | ConvertFrom-Json).fields } | Select-Object -Last 1
        if ($settings.durability -ne $mode -or $settings.journal_mode -ne 'wal' -or $settings.synchronous -ne $(if ($mode -eq 'FULL') { 2 } else { 1 })) { throw 'Broker durability readback did not match requested mode' }
        Compose run --rm -e 'LOAD_PRODUCERS=4' -e 'LOAD_WORKERS=4' -e "LOAD_DURATION_SECONDS=$DurationSeconds" -e 'LOAD_WARMUP_SECONDS=3' -e 'LOAD_PAYLOAD_BYTES=1024' -e 'LOAD_WORK_MS=0' -e "LOAD_RATE=$Rate" -e 'LOAD_DRAIN_SECONDS=120' runner load
        $report = Get-Content (Join-Path $PSScriptRoot 'artifacts/latest.json') -Raw | ConvertFrom-Json
        $report | Add-Member -NotePropertyName brokerDurability -NotePropertyValue $settings
        $report | Add-Member -NotePropertyName databaseFile -NotePropertyValue $env:ANVILMQ_HARNESS_DB_FILE
        $report | ConvertTo-Json -Depth 12 | Set-Content (Join-Path $output "$index-$mode.json")
        Copy-Item (Join-Path $PSScriptRoot 'artifacts/latest.md') (Join-Path $output "$index-$mode.md")
        if (-not $report.passed) { throw "Load test failed: $mode" }
    }
} finally {
    $env:ANVILMQ_DURABILITY = $priorMode
    $env:ANVILMQ_HARNESS_DB_FILE = $priorFile
    # Restore the original broker configuration; retain every test database for inspection.
    Compose up -d --wait broker
}
Write-Host "Durability reports: $output"
