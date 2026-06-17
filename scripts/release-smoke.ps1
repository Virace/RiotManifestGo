param(
    [Parameter(Mandatory = $true)]
    [string]$BinaryPath,

    [string]$OutputDir = ".temp/release-smoke/output"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$binary = (Resolve-Path -LiteralPath $BinaryPath).Path
$manifestPath = Join-Path $repoRoot "pkg/rman/testdata/.cache/riot-manifests/windows/lol-game-client/13.12.5142556_FB988FB8D46FDD43.manifest"
$outputPath = Join-Path $repoRoot $OutputDir

if (-not (Test-Path -LiteralPath $manifestPath)) {
    throw "Fixture manifest cache missing: $manifestPath. Run ./scripts/init-fixtures.ps1 first."
}

if (-not $outputPath.StartsWith($repoRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to use output directory outside repository: $outputPath"
}

if (Test-Path -LiteralPath $outputPath) {
    Remove-Item -LiteralPath $outputPath -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $outputPath | Out-Null

$pattern = "^(content-metadata\.json|code-metadata\.json|DATA/FINAL/Maps/Shipping/CommonLEVELS\.wad\.client)$"

Write-Host "Running release smoke with binary: $binary"
& $binary $manifestPath -p $pattern -o $outputPath -w 1 -retry 1 -s
if ($LASTEXITCODE -ne 0) {
    throw "release smoke CLI download failed with exit code $LASTEXITCODE"
}

$expectedFiles = @(
    @{
        Path = "content-metadata.json"
        Size = 74
        SHA256 = "A222EECBADE03363BE5D194DB067C2DCFF67F2D36292324F6789704915E598C1"
        JSONVersion = "13.12.5142556+branch.releases-13-12.content.release"
    },
    @{
        Path = "code-metadata.json"
        Size = 70
        SHA256 = "3549E3BD230E57EC5B85C78741212B7E4B8E973AB49050FF66C24A78B04B7899"
        JSONVersion = "13.12.5142556+branch.releases-13-12.code.public"
    },
    @{
        Path = "DATA/FINAL/Maps/Shipping/CommonLEVELS.wad.client"
        Size = 308
        SHA256 = "0CD200953E0948CB9C604B59A220DE7640C7C49BB519C7A1F5123E005A73251F"
        JSONVersion = ""
    }
)

foreach ($expected in $expectedFiles) {
    $filePath = Join-Path $outputPath ($expected.Path -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $filePath)) {
        throw "Expected smoke output missing: $($expected.Path)"
    }

    $item = Get-Item -LiteralPath $filePath
    if ($item.Length -ne [int64]$expected.Size) {
        throw "Size mismatch for $($expected.Path): got $($item.Length), want $($expected.Size)"
    }

    $sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $filePath).Hash.ToUpperInvariant()
    if ($sha256 -ne $expected.SHA256) {
        throw "SHA256 mismatch for $($expected.Path): got $sha256, want $($expected.SHA256)"
    }

    if ($expected.JSONVersion -ne "") {
        $json = Get-Content -LiteralPath $filePath -Raw | ConvertFrom-Json
        if ($json.version -ne $expected.JSONVersion) {
            throw "JSON version mismatch for $($expected.Path): got $($json.version), want $($expected.JSONVersion)"
        }
    }

    Write-Host "Verified $($expected.Path) ($($item.Length) bytes)"
}

Write-Host "Release smoke passed."
