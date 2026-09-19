param(
    [ValidatePattern('^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$')][string]$Version = '0.1.0-rc.2',
    [string]$ImageRepository = 'anvilmq',
    [switch]$SkipImageBuild
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Push-Location $root
try {
    $output = Join-Path $root "dist/$Version"
    if (Test-Path $output) { throw "Release directory already exists: $output. Use another version or inspect/remove it before rebuilding." }
    & helm lint charts/anvilmq --strict
    if ($LASTEXITCODE -ne 0) { throw 'Helm lint failed' }
    $revision = (& git rev-parse HEAD).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot determine source revision' }
    $dirty = [bool](& git status --porcelain)
    if (-not $SkipImageBuild) {
        & docker build --platform linux/amd64 --label "org.opencontainers.image.version=$Version" --label "org.opencontainers.image.revision=$revision" -t "${ImageRepository}:$Version" .
        if ($LASTEXITCODE -ne 0) { throw 'Container build failed' }
    }
    New-Item -ItemType Directory -Path $output | Out-Null
    & helm package charts/anvilmq --version $Version --app-version $Version --destination $output
    if ($LASTEXITCODE -ne 0) { throw 'Chart packaging failed' }
    # npm-compatible archive, with the canonical protocol included inside the package.
    $staging = Join-Path $output 'client-staging'
    $package = Join-Path $staging 'package'
    New-Item -ItemType Directory -Path (Join-Path $package 'proto') -Force | Out-Null
    Copy-Item client/src $package -Recurse
    Copy-Item proto/queue.proto (Join-Path $package 'proto/queue.proto')
    Copy-Item client/README.md $package
    $manifest = Get-Content client/package.json -Raw | ConvertFrom-Json
    $manifest.version = $Version
    $manifest.PSObject.Properties.Remove('scripts')
    $manifest.PSObject.Properties.Remove('devDependencies')
    $manifest | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $package 'package.json')
    & tar -czf (Join-Path $output "anvilmq-client-$Version.tgz") -C $staging package
    if ($LASTEXITCODE -ne 0) { throw 'Client packaging failed' }
    Copy-Item proto/queue.proto $output
    Copy-Item deploy/aks-evaluation.yaml $output
    & tar -czf (Join-Path $output "anvilmq-observability-$Version.tgz") -C $root observability docs/observability.md
    if ($LASTEXITCODE -ne 0) { throw 'Observability packaging failed' }
    @{version=$Version; image="${ImageRepository}:$Version"; platform='linux/amd64'; revision=$revision; dirtyWorkingTree=$dirty; imageBuiltLocally=(-not $SkipImageBuild)} | ConvertTo-Json | Set-Content (Join-Path $output 'release.json')
    Get-ChildItem $output -File | Sort-Object Name | ForEach-Object {
        $hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        "$hash  $($_.Name)"
    } | Set-Content (Join-Path $output 'SHA256SUMS')
    Write-Host "Prepared $output. Image/chart/client are not published by this script."
} finally { Pop-Location }
