param([string]$Project = 'anvilmq-harness')
$ErrorActionPreference = 'Stop'
$composeFile = Join-Path $PSScriptRoot 'compose.yaml'
$runId = 'crash-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
$artifacts = Join-Path $PSScriptRoot "artifacts/$runId"
New-Item -ItemType Directory -Path $artifacts | Out-Null
function Compose {
    & docker compose -p $Project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" }
}
function Write-Marker($name, $value) {
    $path = Join-Path $artifacts $name
    $value | ConvertTo-Json -Depth 5 | Set-Content "$path.tmp"
    Move-Item -LiteralPath "$path.tmp" -Destination $path
}
function Wait-Marker($name, $seconds) {
    $end = [DateTime]::UtcNow.AddSeconds($seconds)
    while (-not (Test-Path (Join-Path $artifacts $name))) {
        $process.Refresh()
        if ($process.HasExited) { throw "Runner exited before $name; see $artifacts" }
        if ([DateTime]::UtcNow -ge $end) { throw "Timed out waiting for $name; see $artifacts" }
        Start-Sleep -Milliseconds 100
    }
}
# Unique run directory prevents stale markers. Preserve broker data on every outcome.
Compose build broker runner
Compose up -d --wait broker
$runnerName = "$Project-$runId"
$dockerArgs = @('compose','-p',$Project,'-f',"`"$composeFile`"",'run','--rm','--name',$runnerName,'-e',"CRASH_RUN_ID=$runId",'runner','crash-active')
$process = Start-Process docker -ArgumentList $dockerArgs -PassThru -WindowStyle Hidden -RedirectStandardOutput (Join-Path $artifacts 'runner.log') -RedirectStandardError (Join-Path $artifacts 'runner.stderr.log')
$killed = $false
try {
    Wait-Marker 'ready.json' 30
    $ready = Get-Content (Join-Path $artifacts 'ready.json') -Raw | ConvertFrom-Json
    if (([DateTime]::UtcNow - [DateTime]::Parse($ready.readyAt).ToUniversalTime()).TotalSeconds -gt 10) { throw 'Crash readiness marker is too old' }
    $crashAt = [DateTime]::UtcNow
    Compose kill -s SIGKILL broker
    $killed = $true
    Write-Marker 'killed.json' @{ crashRequestedAt = $crashAt.ToString('o') }
    Wait-Marker 'outage.json' 15
    Compose up -d --wait broker
    $killed = $false
    Write-Marker 'restarted.json' @{ crashRequestedAt = $crashAt.ToString('o'); readyAt = [DateTime]::UtcNow.ToString('o'); crashToReadyMs = ([DateTime]::UtcNow - $crashAt).TotalMilliseconds }
    Wait-Marker 'report.json' 120
    if (-not $process.WaitForExit(10000)) { throw 'Runner did not exit after reporting' }
    Get-Content (Join-Path $artifacts 'report.md')
    $report = Get-Content (Join-Path $artifacts 'report.json') -Raw | ConvertFrom-Json
    if ($process.ExitCode -ne 0 -or -not $report.passed) { throw "Active-crash verification failed; see $artifacts" }
    Write-Host "Reports: $artifacts"
} finally {
    if ($killed) { Compose up -d --wait broker }
    $process.Refresh()
    if (-not $process.HasExited) {
        # Only this invocation's runner; never remove the broker volume.
        & docker stop --time 2 $runnerName | Out-Null
    }
}
