param(
    [switch]$ForceDownload
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$metadataPath = Join-Path $repoRoot "pkg/rman/testdata/fixtures/riot-manifests/fixtures.json"

if (-not (Test-Path -LiteralPath $metadataPath)) {
    throw "Fixture metadata not found: $metadataPath"
}

$metadata = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json
$cacheRoot = Join-Path $repoRoot $metadata.cache_dir
$upstreamRoot = Join-Path $repoRoot ".temp/riot-manifests"
$resolvedPath = Join-Path $cacheRoot "resolved-fixtures.json"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
    }
}

function ConvertTo-NormalizedRelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BasePath,
        [Parameter(Mandatory = $true)]
        [string]$FullPath
    )

    # [Uri]::MakeRelativeUri is not usable here: POSIX absolute paths parse as
    # relative URIs and the call throws on Linux runners.
    $relative = [IO.Path]::GetRelativePath($BasePath, $FullPath)
    return $relative -replace "\\", "/"
}

function Get-VersionParts {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    $stem = [IO.Path]::GetFileNameWithoutExtension($FileName)
    $version = ($stem -split "_")[0]
    [int[]]$parts = @($version -split "\." | ForEach-Object { [int]$_ })

    while ($parts.Count -lt 6) {
        $parts += 0
    }

    return $parts
}

function Get-ManifestIDFromURL {
    param(
        [Parameter(Mandatory = $true)]
        [string]$URL
    )

    $uri = [Uri]$URL
    return [IO.Path]::GetFileNameWithoutExtension($uri.Segments[-1])
}

function Get-VersionFromIndexName {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FileName
    )

    $stem = [IO.Path]::GetFileNameWithoutExtension($FileName)
    return ($stem -split "_")[0]
}

function Ensure-UpstreamIndex {
    if (Test-Path -LiteralPath (Join-Path $upstreamRoot ".git")) {
        Invoke-Checked git -C $upstreamRoot fetch --depth=1 --quiet origin $metadata.source_ref
        Invoke-Checked git -C $upstreamRoot -c advice.detachedHead=false checkout --force --quiet FETCH_HEAD
        Invoke-Checked git -C $upstreamRoot sparse-checkout set $metadata.default_region
        return
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $upstreamRoot) | Out-Null
    Invoke-Checked git clone --depth=1 --filter=blob:none --sparse --quiet $metadata.source_repository $upstreamRoot
    Invoke-Checked git -C $upstreamRoot sparse-checkout set $metadata.default_region
}

function Resolve-FixedFixture {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Fixture
    )

    $indexPath = Join-Path $upstreamRoot ($Fixture.upstream_index_path -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $indexPath)) {
        throw "Upstream index file not found: $($Fixture.upstream_index_path)"
    }

    $cdnURL = ([IO.File]::ReadAllText($indexPath)).Trim()
    if ($cdnURL -ne $Fixture.cdn_url) {
        throw "Pinned CDN URL mismatch for $($Fixture.name): metadata=$($Fixture.cdn_url), upstream=$cdnURL"
    }

    return [pscustomobject]@{
        name                = $Fixture.name
        slot                = $Fixture.slot
        platform            = $Fixture.platform
        product             = $Fixture.product
        version             = $Fixture.version
        upstream_index_path = $Fixture.upstream_index_path
        manifest_id         = $Fixture.manifest_id
        cdn_url             = $cdnURL
        expected_size       = [int64]$Fixture.size
        expected_sha256     = [string]$Fixture.sha256
    }
}

function Resolve-LatestFixture {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Fixture
    )

    $selector = $Fixture.latest_selector
    $selectorDir = Split-Path -Parent $selector
    $selectorFilter = Split-Path -Leaf $selector
    $searchDir = Join-Path $upstreamRoot ($selectorDir -replace "/", [IO.Path]::DirectorySeparatorChar)

    if (-not (Test-Path -LiteralPath $searchDir)) {
        throw "Latest selector directory not found: $selectorDir"
    }

    $latestIndex = Get-ChildItem -Path $searchDir -Filter $selectorFilter -File |
        Sort-Object `
            @{ Expression = { (Get-VersionParts $_.Name)[0] } }, `
            @{ Expression = { (Get-VersionParts $_.Name)[1] } }, `
            @{ Expression = { (Get-VersionParts $_.Name)[2] } }, `
            @{ Expression = { (Get-VersionParts $_.Name)[3] } }, `
            @{ Expression = { (Get-VersionParts $_.Name)[4] } }, `
            @{ Expression = { (Get-VersionParts $_.Name)[5] } }, `
            Name |
        Select-Object -Last 1

    if ($null -eq $latestIndex) {
        throw "No files matched latest selector: $selector"
    }

    $cdnURL = ([IO.File]::ReadAllText($latestIndex.FullName)).Trim()
    $manifestID = Get-ManifestIDFromURL $cdnURL
    $relativeIndexPath = ConvertTo-NormalizedRelativePath -BasePath $upstreamRoot -FullPath $latestIndex.FullName

    return [pscustomobject]@{
        name                = $Fixture.name
        slot                = $Fixture.slot
        platform            = $Fixture.platform
        product             = $Fixture.product
        version             = Get-VersionFromIndexName $latestIndex.Name
        upstream_index_path = $relativeIndexPath
        manifest_id         = $manifestID
        cdn_url             = $cdnURL
        expected_size       = 0
        expected_sha256     = ""
    }
}

function Get-LocalManifestPath {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Resolved
    )

    $fileName = "$($Resolved.version)_$($Resolved.manifest_id).manifest"
    return Join-Path $cacheRoot (Join-Path $Resolved.platform (Join-Path $Resolved.product $fileName))
}

function Download-And-VerifyManifest {
    param(
        [Parameter(Mandatory = $true)]
        [pscustomobject]$Resolved
    )

    $localPath = Get-LocalManifestPath $Resolved
    $localDir = Split-Path -Parent $localPath
    New-Item -ItemType Directory -Force -Path $localDir | Out-Null

    if ($ForceDownload -or -not (Test-Path -LiteralPath $localPath)) {
        Write-Host "Downloading $($Resolved.name): $($Resolved.cdn_url)"
        Invoke-WebRequest -Uri $Resolved.cdn_url -OutFile $localPath -TimeoutSec 180
    }

    $file = Get-Item -LiteralPath $localPath
    if ($Resolved.expected_size -gt 0 -and $file.Length -ne $Resolved.expected_size) {
        throw "Size mismatch for $($Resolved.name): got $($file.Length), want $($Resolved.expected_size)"
    }

    $sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $localPath).Hash.ToUpperInvariant()
    if ($Resolved.expected_sha256 -ne "" -and $sha256 -ne $Resolved.expected_sha256.ToUpperInvariant()) {
        throw "SHA256 mismatch for $($Resolved.name): got $sha256, want $($Resolved.expected_sha256)"
    }

    $relativeLocalPath = ConvertTo-NormalizedRelativePath -BasePath $repoRoot -FullPath $localPath
    return [pscustomobject]@{
        name                = $Resolved.name
        slot                = $Resolved.slot
        platform            = $Resolved.platform
        product             = $Resolved.product
        version             = $Resolved.version
        upstream_index_path = $Resolved.upstream_index_path
        manifest_id         = $Resolved.manifest_id
        cdn_url             = $Resolved.cdn_url
        local_path          = $relativeLocalPath
        size                = $file.Length
        sha256              = $sha256
    }
}

Ensure-UpstreamIndex
New-Item -ItemType Directory -Force -Path $cacheRoot | Out-Null

$resolvedFixtures = @()
foreach ($fixture in $metadata.fixtures) {
    if ($fixture.PSObject.Properties.Name -contains "latest_selector") {
        $resolved = Resolve-LatestFixture $fixture
    } else {
        $resolved = Resolve-FixedFixture $fixture
    }

    $verified = Download-And-VerifyManifest $resolved
    $resolvedFixtures += $verified
    Write-Host ("{0} {1} {2} {3} bytes" -f $verified.slot, $verified.version, $verified.manifest_id, $verified.size)
}

$resolvedDoc = [pscustomobject]@{
    generated_at_utc = [DateTime]::UtcNow.ToString("o")
    source_repository = $metadata.source_repository
    source_ref = $metadata.source_ref
    fixtures = $resolvedFixtures
}

$resolvedDoc | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedPath -Encoding UTF8
Write-Host "Resolved fixture metadata written to $resolvedPath"
