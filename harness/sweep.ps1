param([int[]]$Rates = @(100,200,400,800), [string]$Label = 'sweep')
$ErrorActionPreference = 'Stop'
$compose = Join-Path $PSScriptRoot 'compose.yaml'
$artifacts = Join-Path $PSScriptRoot 'artifacts'
foreach ($rate in $Rates) {
    $stem = Join-Path $artifacts "$Label-$rate"
    $dockerArgs = @('compose','-p','anvilmq-harness','-f',$compose,'run','--rm','-e','LOAD_PRODUCERS=4','-e','LOAD_WORKERS=4','-e','LOAD_DURATION_SECONDS=60','-e','LOAD_WARMUP_SECONDS=3','-e',"LOAD_RATE=$rate",'runner','load')
    $process = Start-Process docker -ArgumentList $dockerArgs -PassThru -WindowStyle Hidden -RedirectStandardOutput "$stem.log" -RedirectStandardError "$stem.stderr.log"
    $samples = [System.Collections.Generic.List[object]]::new()
    while (-not $process.HasExited) {
        $raw = & docker stats --no-stream --format '{{json .}}'
        foreach ($line in $raw) {
            $stat = $line | ConvertFrom-Json
            if ($stat.Name -like 'anvilmq-harness-*') {
                $samples.Add([pscustomobject]@{ timestamp = [DateTime]::UtcNow.ToString('o'); data = $stat })
            }
        }
        $process.Refresh()
    }
    $process.WaitForExit()
    $samples | ConvertTo-Json -Depth 5 | Set-Content "$stem-resources.json"
    Get-Content "$stem.log"
    if ($process.ExitCode -ne 0) { throw "Rate $rate failed; see $stem.stderr.log" }
    Copy-Item (Join-Path $artifacts 'latest.json') "$stem.json"
    Copy-Item (Join-Path $artifacts 'latest.md') "$stem.md"
}
