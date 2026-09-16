param([string]$Project = 'anvilmq-harness')
$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
function Compose {
    & docker compose -p $Project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" }
}
# Preserve data and containers on failure for investigation; never delete volumes here.
Compose build broker runner
Compose up -d --wait broker
Compose run --rm runner smoke
Compose run --rm runner seed
Compose kill -s SIGKILL broker
Compose up -d --wait broker
Compose run --rm runner verify
Write-Host "PASS: lifecycle and process-crash persistence. Reports: $PSScriptRoot/artifacts"
Write-Host "Stop: docker compose -p $Project -f `"$composeFile`" down (preserves volume)"
