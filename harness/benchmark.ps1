#requires -Version 7
# Isolation benchmark orchestrator. Builds the broker and runner images once, then runs three
# variable-isolated scenario sets, each repetition from a CLEAN database (a fresh unique
# ANVILMQ_HARNESS_DB_FILE with a broker container recreate — no image rebuild), pinning and
# recording the environment. Per run it samples Docker CPU/memory, captures broker logs, and
# copies the machine-readable load report. Finally it aggregates repetitions via summarize.ts.
#
#   Set A  isolate ARRIVAL RATE  : fixed producers/workers, sweep -Rates.
#   Set B  isolate CONCURRENCY   : fixed -FixedRate and producer count (held equal to Set A so
#                                  worker count is the only variable), sweep -WorkerCounts.
#   Set D  durability comparison : fixed operating point, NORMAL vs FULL.
#
# Runs sequentially against one dedicated broker (completion checks assume a dedicated instance).
# Pass -OutDir to append to an existing run directory (e.g. run Set A, read the bracket, then B/D).
param(
    [string]$Project = 'anvilmq-bench',
    [string]$OutDir,
    [ValidateSet('A','B','D','summary')][string[]]$Sets = @('A','B','D','summary'),
    [int[]]$Rates = @(200,250,300,350,400),
    [ValidateRange(1,128)][int]$Producers = 4,
    [ValidateRange(1,128)][int]$Workers = 4,
    [ValidateRange(1,100000)][int]$FixedRate = 250,
    [int[]]$WorkerCounts = @(1,2,4,8),
    [ValidateRange(1,128)][int]$SetBProducers = 4,
    [ValidateRange(1,128)][int]$DurWorkers = 4,
    [ValidateSet('NORMAL','FULL')][string[]]$Durability = @('NORMAL','FULL'),
    [ValidateRange(1,10)][int]$Reps = 3,
    [ValidateRange(1,300)][int]$DurationSeconds = 60,
    [ValidateRange(0,60)][int]$WarmupSeconds = 5,
    [ValidateRange(1,600)][int]$DrainSeconds = 120,
    [ValidateRange(0,1000000)][int]$PayloadBytes = 1024,
    [ValidateRange(0,30000)][int]$WorkMs = 0,
    [int]$GrpcPort = 50072,
    [int]$HttpPort = 9099,
    [switch]$SkipBuild
)
$ErrorActionPreference = 'Stop'
$compose = Join-Path $PSScriptRoot 'compose.yaml'
$artifacts = Join-Path $PSScriptRoot 'artifacts'
$runId = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmss') + '-' + [Guid]::NewGuid().ToString('N').Substring(0,8)
if (-not $OutDir) { $OutDir = Join-Path $artifacts "bench-$runId" }
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

# Distinct host ports so a concurrently running smoke harness does not clash; the runner reaches
# the broker over the Compose network regardless.
$env:ANVILMQ_HARNESS_GRPC_PORT = "$GrpcPort"
$env:ANVILMQ_HARNESS_HTTP_PORT = "$HttpPort"
$priorMode = $env:ANVILMQ_DURABILITY
$priorFile = $env:ANVILMQ_HARNESS_DB_FILE

function Compose { & docker compose -p $Project -f $compose @args; if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $args" } }

function Invoke-Run {
    param([string]$Stem, [string]$DbFile, [string]$Mode, [int]$Prod, [int]$Work, [int]$Rate)
    $env:ANVILMQ_HARNESS_DB_FILE = $DbFile
    $env:ANVILMQ_DURABILITY = $Mode
    Compose up -d --wait broker
    $stemPath = Join-Path $OutDir $Stem
    $envArgs = @('-e',"LOAD_PRODUCERS=$Prod",'-e',"LOAD_WORKERS=$Work",
        '-e',"LOAD_DURATION_SECONDS=$DurationSeconds",'-e',"LOAD_WARMUP_SECONDS=$WarmupSeconds",
        '-e',"LOAD_PAYLOAD_BYTES=$PayloadBytes",'-e',"LOAD_WORK_MS=$WorkMs",
        '-e',"LOAD_RATE=$Rate",'-e',"LOAD_DRAIN_SECONDS=$DrainSeconds")
    # Sample Docker CPU/memory in a background job (appending one JSON object per line) so the
    # load run can execute in the foreground; the call operator preserves the space in $compose.
    $jsonl = "$stemPath-resources.jsonl"
    Remove-Item $jsonl -ErrorAction SilentlyContinue
    $latestJson = Join-Path $artifacts 'latest.json'
    $before = (Get-Item $latestJson -ErrorAction SilentlyContinue).LastWriteTimeUtc
    $sampler = Start-Job -ScriptBlock {
        param($proj, $file)
        while ($true) {
            $now = [DateTime]::UtcNow.ToString('o')
            foreach ($line in (& docker stats --no-stream --format '{{json .}}')) {
                $stat = $line | ConvertFrom-Json
                if ($stat.Name -like "$proj-*") { ([pscustomobject]@{ timestamp = $now; data = $stat } | ConvertTo-Json -Depth 6 -Compress) | Add-Content -Path $file }
            }
        }
    } -ArgumentList $Project, $jsonl
    try {
        & docker compose -p $Project -f $compose run --rm @envArgs runner load *>&1 | Tee-Object -FilePath "$stemPath.log" | Out-Null
        $exit = $LASTEXITCODE
    } finally {
        Stop-Job $sampler -ErrorAction SilentlyContinue; Remove-Job $sampler -Force -ErrorAction SilentlyContinue
    }
    $lines = @(Get-Content $jsonl -ErrorAction SilentlyContinue)
    "[" + ($lines -join ",`n") + "]" | Set-Content "$stemPath-resources.json"
    Remove-Item $jsonl -ErrorAction SilentlyContinue
    # Record only the broker's durability readback and a short tail; per-job info logs are huge.
    $brokerLog = Compose logs --no-log-prefix broker
    ($brokerLog | Where-Object { $_ -match 'durability configured' } | Select-Object -Last 1) | Set-Content "$stemPath-broker-config.log"
    ($brokerLog | Select-Object -Last 20) | Set-Content "$stemPath-broker-tail.log"
    # A completed measurement that fails only its correctness/drain check (an expected outcome for
    # an overloaded, non-sustained point) still writes latest.json and exits 1. Treat that as a
    # valid data point and continue; only a setup/transport error (no fresh report) aborts.
    $latest = Get-Item $latestJson -ErrorAction SilentlyContinue
    $produced = $latest -and (-not $before -or $latest.LastWriteTimeUtc -gt $before)
    if (-not $produced) { Get-Content "$stemPath.log" -ErrorAction SilentlyContinue | Select-Object -Last 20; throw "Run $Stem produced no report (exit $exit); see $stemPath.log" }
    Copy-Item $latestJson "$stemPath.json"
    Copy-Item (Join-Path $artifacts 'latest.md') "$stemPath.md"
    $r = Get-Content "$stemPath.json" -Raw | ConvertFrom-Json
    Write-Host ("  {0}: enqueue {1:n1}/s, completion(win) {2:n1}/s, e2e p99 {3:n1}ms, backlog slope {4:n2}, sustained={5}, passed={6}" -f `
        $Stem, $r.enqueuePerSecond, $r.completionsPerSecondDuringWindow, $r.latencyMs.submissionToCompletionAck.p99, $r.backlogSlopePerSecond, $r.sustained, $r.passed)
}

try {
    if (-not $SkipBuild) { Compose build broker runner }
    & docker info --format '{{json .}}' | Set-Content (Join-Path $OutDir 'docker-info.json')
    (Compose images --format json) | Set-Content (Join-Path $OutDir 'images.json')
    [pscustomobject]@{
        runId = $runId; sets = $Sets; rates = $Rates; producers = $Producers; workers = $Workers;
        fixedRate = $FixedRate; workerCounts = $WorkerCounts; setBProducers = $SetBProducers;
        durWorkers = $DurWorkers; durability = $Durability; reps = $Reps; durationSeconds = $DurationSeconds;
        warmupSeconds = $WarmupSeconds; drainSeconds = $DrainSeconds; payloadBytes = $PayloadBytes; workMs = $WorkMs
    } | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $OutDir 'manifest.json')

    if ($Sets -contains 'A') {
        Write-Host "== Set A: arrival-rate sweep, fixed $Producers producers / $Workers workers, NORMAL =="
        foreach ($rate in $Rates) { for ($rep = 1; $rep -le $Reps; $rep++) {
            Invoke-Run -Stem "A-rate$rate-rep$rep" -DbFile "$runId-A-$rate-$rep.db" -Mode 'NORMAL' -Prod $Producers -Work $Workers -Rate $rate } }
    }
    if ($Sets -contains 'B') {
        Write-Host "== Set B: worker-concurrency sweep, fixed $FixedRate jobs/s, $SetBProducers producers, NORMAL =="
        foreach ($w in $WorkerCounts) { for ($rep = 1; $rep -le $Reps; $rep++) {
            Invoke-Run -Stem "B-workers$w-rep$rep" -DbFile "$runId-B-$w-$rep.db" -Mode 'NORMAL' -Prod $SetBProducers -Work $w -Rate $FixedRate } }
    }
    if ($Sets -contains 'D') {
        Write-Host "== Set D: durability comparison at $FixedRate jobs/s, $Producers producers / $DurWorkers workers =="
        foreach ($mode in $Durability) { for ($rep = 1; $rep -le $Reps; $rep++) {
            Invoke-Run -Stem "D-$mode-rep$rep" -DbFile "$runId-D-$mode-$rep.db" -Mode $mode -Prod $Producers -Work $DurWorkers -Rate $FixedRate } }
    }
    if ($Sets -contains 'summary') {
        Write-Host "== Aggregating repetitions =="
        & bun (Join-Path $PSScriptRoot 'summarize.ts') $OutDir
        if ($LASTEXITCODE -ne 0) { throw 'summarize.ts failed' }
    }
} finally {
    $env:ANVILMQ_DURABILITY = $priorMode
    $env:ANVILMQ_HARNESS_DB_FILE = $priorFile
}
Write-Host "Benchmark artifacts: $OutDir"
