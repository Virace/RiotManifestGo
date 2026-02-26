param (
    [string]$Version = "dev",
    [string]$Commit = $(git rev-parse --short HEAD 2>$null),
    [string]$Os = "windows",
    [string]$Arch = "amd64"
)

if (-not $Commit) { $Commit = "unknown" }
$OutputDir = "build"
if (-not (Test-Path $OutputDir)) { New-Item -ItemType Directory -Path $OutputDir | Out-Null }

$AppName = "manifest-cli"
$Ext = ""
if ($Os -eq "windows") { $Ext = ".exe" }
$OutputBinary = Join-Path $OutputDir "${AppName}-${Os}-${Arch}${Ext}"

Write-Host ">>> Building RiotManifestGo" -ForegroundColor Cyan
Write-Host "    Version: $Version"
Write-Host "    Commit:  $Commit"
Write-Host "    OS/Arch: $Os/$Arch"
Write-Host "    Output:  $OutputBinary"

$LdFlags = "-s -w -X main.version=$Version -X main.commit=$Commit"
$env:GOOS = $Os
$env:GOARCH = $Arch

Write-Host "`n>>> [1/2] Compiling with Go..." -ForegroundColor Yellow
$compileStartTime = Get-Date

go build -ldflags $LdFlags -o $OutputBinary ./cmd/manifest-cli

if ($LASTEXITCODE -ne 0) {
    Write-Host "Compile failed!" -ForegroundColor Red
    exit 1
}

$compileTime = (Get-Date) - $compileStartTime
$fileInfo = Get-Item $OutputBinary
$rawSize = [math]::Round($fileInfo.Length / 1MB, 2)
Write-Host ("Compile success! Time: {0:0.0}s | Raw Size: {1}MB" -f $compileTime.TotalSeconds, $rawSize) -ForegroundColor Green

$hasUpx = Get-Command upx -ErrorAction SilentlyContinue
if ($hasUpx) {
    Write-Host "`n>>> [2/2] Compressing with UPX..." -ForegroundColor Yellow
    $upxStartTime = Get-Date
    
    upx --best --lzma $OutputBinary | Out-Null
    
    if ($LASTEXITCODE -eq 0) {
        $upxTime = (Get-Date) - $upxStartTime
        $fileInfo.Refresh()
        $compressedSize = [math]::Round($fileInfo.Length / 1MB, 2)
        $ratio = [math]::Round(($compressedSize / $rawSize) * 100, 1)
        Write-Host ("UPX success! Time: {0:0.0}s | Compressed Size: {1}MB ({2}%)" -f $upxTime.TotalSeconds, $compressedSize, $ratio) -ForegroundColor Green
    }
    else {
        Write-Host "UPX warning/error, skipping." -ForegroundColor Yellow
    }
}
else {
    Write-Host "`n>>> [2/2] UPX not found, skipping compression." -ForegroundColor Magenta
}

Write-Host "`nPipeline finished successfully!`n" -ForegroundColor Cyan
exit 0
