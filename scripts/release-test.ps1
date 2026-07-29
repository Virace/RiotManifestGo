# Pre-release full test: unit/component tests + real-network download & update E2E.
#
# Mirrors PyManifest scripts/e2e_update_real.py using the CLI binary:
#   1. go test ./...                     (unit & component tests)
#   2. fixed Riot CDN multi-Range integration (live manifest + bundle data)
#   3. go test -tags=fixtures ./pkg/rman (offline fixture parser tests)
#   4. E2E against two pinned manifest versions from the fixture cache:
#        managed install (10.9) -> corrupt + repair -> managed update (13.12),
#      each step finished with a chunk-level -verify-only pass (exit code based),
#      plus a parse smoke on the latest resolved manifest.
#
# Usage:
#   ./scripts/release-test.ps1                          # build CLI from source
#   ./scripts/release-test.ps1 -BinaryPath ./build/manifest-cli-windows-amd64.exe
#   ./scripts/release-test.ps1 -SkipGoTests             # E2E only

param(
    [string]$BinaryPath = "",
    [string]$OutputDir = ".temp/release-test/output",
    [switch]$SkipGoTests
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$cacheRoot = Join-Path $repoRoot "pkg/rman/testdata/.cache/riot-manifests"
$resolvedPath = Join-Path $cacheRoot "resolved-fixtures.json"

# Pinned fixture slots (see pkg/rman/testdata/fixtures/riot-manifests/fixtures.json).
$oldSlot = "fixed-old"                # 10.9.3185929  037EC59D5BD7C5D3
$newSlot = "fixed-middle"             # 13.12.5142556 FB988FB8D46FDD43
$latestSlot = "latest-release-check"  # dynamic, resolved by init-fixtures.ps1

# Files present in both pinned versions: two tiny metadata files plus three
# small multi-chunk wads so the update step exercises chunk-level verify/reuse.
$targetPattern = '^(content-metadata\.json|code-metadata\.json|DATA/FINAL/Champions/(Alistar|Amumu|Annie)\.zh_CN\.wad\.client)$'
$targetFiles = @(
    "content-metadata.json",
    "code-metadata.json",
    "DATA/FINAL/Champions/Alistar.zh_CN.wad.client",
    "DATA/FINAL/Champions/Amumu.zh_CN.wad.client",
    "DATA/FINAL/Champions/Annie.zh_CN.wad.client"
)
$corruptTarget = "DATA/FINAL/Champions/Alistar.zh_CN.wad.client"
$newContentVersion = "13.12.5142556+branch.releases-13-12.content.release"

function Invoke-CLI {
    param([string[]]$CliArgs)

    # Out-Host keeps CLI stdout on the console; otherwise it would be captured
    # into the function's return value alongside the exit code.
    & $script:binary @CliArgs | Out-Host
    return $LASTEXITCODE
}

function Assert-CLISucceeded {
    param([string[]]$CliArgs, [string]$What)

    $code = Invoke-CLI $CliArgs
    if ($code -ne 0) {
        throw "$What failed with exit code $code"
    }
}

function ConvertFrom-HumanSize {
    param([string]$Text)

    if ($Text -notmatch '^([\d.]+)\s*(B|KB|MB|GB|TB)$') {
        throw "Unrecognized size text: $Text"
    }
    $multiplier = @{ B = 1; KB = 1KB; MB = 1MB; GB = 1GB; TB = 1TB }[$Matches[2]]
    return [int64]([double]$Matches[1] * $multiplier)
}

# Extracts a byte counter from the CLI -log file by its label line,
# e.g. Get-LogBytes update.log '网络下载' for downloaded bytes.
function Get-LogBytes {
    param([string]$LogPath, [string]$Label)

    $line = Get-Content -LiteralPath $LogPath -Encoding UTF8 |
        Where-Object { $_ -match "^\s*$($Label): (.+)$" } |
        Select-Object -First 1
    if ($null -eq $line) {
        throw "Label '$Label' not found in log: $LogPath"
    }
    $null = $line -match "^\s*$($Label): (.+)$"
    return ConvertFrom-HumanSize $Matches[1].Trim()
}

function Get-InstalledManifestID {
    $installedPath = Join-Path $outputPath ".rman/installed.json"
    if (-not (Test-Path -LiteralPath $installedPath)) {
        throw "installed.json missing: $installedPath"
    }
    $state = Get-Content -LiteralPath $installedPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ([int]$state.schema -ne 2 -or $null -eq $state.files) {
        throw "installed.json must use schema 2 with files coverage"
    }
    return [string]$state.manifest_id
}

function Assert-InstalledManifestID {
    param([string]$Expected, [string]$Step)

    $got = Get-InstalledManifestID
    if ($got -ne $Expected) {
        throw "installed.json manifest_id after ${Step}: got $got, want $Expected"
    }
}

function Get-FixtureBySlot {
    param([pscustomobject]$Resolved, [string]$Slot)

    $fixture = $Resolved.fixtures | Where-Object { $_.slot -eq $Slot } | Select-Object -First 1
    if ($null -eq $fixture) {
        throw "Fixture slot not found in resolved-fixtures.json: $Slot"
    }
    return $fixture
}

# ---- Phase 1/5: unit & component tests ----

Push-Location $repoRoot
try {
    if ($SkipGoTests) {
        Write-Host "== [1/5] go test skipped (-SkipGoTests)" -ForegroundColor Yellow
    } else {
        Write-Host "== [1/5] go test ./... -count=1" -ForegroundColor Cyan
        go test ./... -count=1
        if ($LASTEXITCODE -ne 0) { throw "go test ./... failed" }

        Write-Host "Running fixed Riot CDN multi-Range integration..." -ForegroundColor Cyan
        go test -tags=integration ./internal/netpool `
            -run '^TestRiotCDNDefaultAssetsMultiRange$' `
            -count=1 `
            -timeout=6m
        if ($LASTEXITCODE -ne 0) { throw "Riot CDN multi-Range integration failed" }
    }

    # ---- Phase 2/5: fixture cache + offline fixture tests ----

    Write-Host "== [2/5] fixture cache + offline fixture tests" -ForegroundColor Cyan
    if (-not (Test-Path -LiteralPath $resolvedPath)) {
        Write-Host "Fixture cache missing, running init-fixtures.ps1..."
        & (Join-Path $PSScriptRoot "init-fixtures.ps1")
    }
    $resolved = Get-Content -LiteralPath $resolvedPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $oldFixture = Get-FixtureBySlot $resolved $oldSlot
    $newFixture = Get-FixtureBySlot $resolved $newSlot
    $latestFixture = Get-FixtureBySlot $resolved $latestSlot
    $oldManifest = Join-Path $repoRoot $oldFixture.local_path
    $newManifest = Join-Path $repoRoot $newFixture.local_path
    $latestManifest = Join-Path $repoRoot $latestFixture.local_path
    foreach ($path in @($oldManifest, $newManifest, $latestManifest)) {
        if (-not (Test-Path -LiteralPath $path)) {
            throw "Fixture manifest cache missing: $path. Run ./scripts/init-fixtures.ps1 first."
        }
    }

    if ($SkipGoTests) {
        Write-Host "offline fixture tests skipped (-SkipGoTests)" -ForegroundColor Yellow
    } else {
        go test -tags=fixtures ./pkg/rman -count=1
        if ($LASTEXITCODE -ne 0) { throw "go test -tags=fixtures ./pkg/rman failed" }
    }

    # ---- Phase 3/5: CLI binary ----

    Write-Host "== [3/5] CLI binary" -ForegroundColor Cyan
    if ($BinaryPath -ne "") {
        $binary = (Resolve-Path -LiteralPath $BinaryPath).Path
    } else {
        $ext = if ($env:OS -eq "Windows_NT") { ".exe" } else { "" }
        $binary = Join-Path $repoRoot ".temp/release-test/manifest-cli$ext"
        New-Item -ItemType Directory -Force -Path (Split-Path -Parent $binary) | Out-Null
        go build -o $binary ./cmd/manifest-cli
        if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/manifest-cli failed" }
    }
    Write-Host "Using binary: $binary"

    # ---- Phase 4/5: real-network E2E (full download -> repair -> update) ----

    Write-Host "== [4/5] real-network E2E ($($oldFixture.version) -> $($newFixture.version))" -ForegroundColor Cyan
    $outputPath = Join-Path $repoRoot $OutputDir
    if (-not $outputPath.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to use output directory outside repository: $outputPath"
    }
    if (Test-Path -LiteralPath $outputPath) {
        Remove-Item -LiteralPath $outputPath -Recurse -Force
    }
    New-Item -ItemType Directory -Force -Path $outputPath | Out-Null
    $logDir = Join-Path $repoRoot ".temp/release-test"

    # Step 1: managed installation of the old version into an empty directory.
    Write-Host "-- E2E step 1: managed install $($oldFixture.version)"
    Assert-CLISucceeded @($oldManifest, "-p", $targetPattern, "-o", $outputPath, "-install", "-w", "4", "-retry", "2", "-s") "managed install"
    foreach ($rel in $targetFiles) {
        $filePath = Join-Path $outputPath ($rel -replace "/", [IO.Path]::DirectorySeparatorChar)
        if (-not (Test-Path -LiteralPath $filePath)) {
            throw "Expected file missing after managed install: $rel"
        }
    }
    Assert-InstalledManifestID $oldFixture.manifest_id "managed install"
    Assert-CLISucceeded @($oldManifest, "-p", $targetPattern, "-o", $outputPath, "-verify-only", "-s") "post-download verify"
    Write-Host "managed install OK, installed=$($oldFixture.manifest_id)"

    # Step 2: corrupt one file in the middle, expect verify to flag it and
    # repair to re-download only the broken chunks.
    Write-Host "-- E2E step 2: corrupt + repair"
    $corruptPath = Join-Path $outputPath ($corruptTarget -replace "/", [IO.Path]::DirectorySeparatorChar)
    $cleanHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $corruptPath).Hash
    $corruptSize = (Get-Item -LiteralPath $corruptPath).Length
    $stream = [IO.File]::Open($corruptPath, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite)
    try {
        $null = $stream.Seek([long]($corruptSize / 2), [IO.SeekOrigin]::Begin)
        $junk = [byte[]]::new(16)
        for ($i = 0; $i -lt $junk.Length; $i++) { $junk[$i] = [byte][char]'X' }
        $stream.Write($junk, 0, $junk.Length)
    } finally {
        $stream.Close()
    }

    $verifyCode = Invoke-CLI @($oldManifest, "-p", $targetPattern, "-o", $outputPath, "-verify-only", "-s")
    if ($verifyCode -eq 0) {
        throw "verify-only should have detected the corrupted file but exited 0"
    }

    $repairLog = Join-Path $logDir "repair.log"
    Assert-CLISucceeded @($oldManifest, "-p", $targetPattern, "-o", $outputPath, "-install", "-repair", "-log", $repairLog, "-w", "4", "-retry", "2", "-s") "repair"
    $repairedBytes = Get-LogBytes $repairLog '网络下载'
    if ($repairedBytes -le 0 -or $repairedBytes -ge $corruptSize) {
        throw "repair should download only the broken chunks: downloaded=$repairedBytes, file size=$corruptSize"
    }
    $repairedHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $corruptPath).Hash
    if ($repairedHash -ne $cleanHash) {
        throw "repair did not restore file content: got $repairedHash, want $cleanHash"
    }
    Assert-CLISucceeded @($oldManifest, "-p", $targetPattern, "-o", $outputPath, "-verify-only", "-s") "post-repair verify"
    Write-Host "repair OK, downloaded $repairedBytes of $corruptSize bytes"

    # Step 3: incremental update to the new version; the old manifest is
    # auto-discovered from the installed.json archive written in step 1.
    Write-Host "-- E2E step 3: incremental update to $($newFixture.version)"
    $updateLog = Join-Path $logDir "update.log"
    Assert-CLISucceeded @($newManifest, "-p", $targetPattern, "-o", $outputPath, "-install", "-log", $updateLog, "-w", "4", "-retry", "2", "-s") "incremental update"
    $updateDownloaded = Get-LogBytes $updateLog '网络下载'
    if ($updateDownloaded -le 0) {
        throw "cross-version update should download new content, downloaded=$updateDownloaded"
    }
    Assert-InstalledManifestID $newFixture.manifest_id "incremental update"
    Assert-CLISucceeded @($newManifest, "-p", $targetPattern, "-o", $outputPath, "-verify-only", "-s") "post-update verify"

    $contentMeta = Get-Content -LiteralPath (Join-Path $outputPath "content-metadata.json") -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($contentMeta.version -ne $newContentVersion) {
        throw "content-metadata.json version after update: got $($contentMeta.version), want $newContentVersion"
    }
    Write-Host "update OK, downloaded $updateDownloaded bytes, installed=$($newFixture.manifest_id)"

    # Step 4: parser compatibility smoke against the latest resolved manifest.
    Write-Host "-- E2E step 4: latest manifest parse smoke ($($latestFixture.version))"
    Assert-CLISucceeded @($latestManifest, "-list", "-n", "1", "-p", "content-metadata") "latest manifest list"

    # ---- Phase 5/5: summary ----

    Write-Host "== [5/5] release test passed" -ForegroundColor Green
    Write-Host "  go tests:  $(if ($SkipGoTests) { 'skipped' } else { 'passed' })"
    Write-Host "  E2E:       $($oldFixture.version) install -> repair -> $($newFixture.version) update -> verify"
    Write-Host "  latest:    $($latestFixture.version) parse smoke"
} finally {
    Pop-Location
}
