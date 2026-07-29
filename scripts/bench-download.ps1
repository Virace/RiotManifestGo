#Requires -Version 7
<#
真实网络下载基准：多轮运行 manifest-cli 记录耗时；-PlanOnly 时转为零流量计划对照。
用法:
  pwsh scripts/bench-download.ps1 -Manifest .\game.manifest -Rounds 3
  pwsh scripts/bench-download.ps1 -Manifest <url> -CliArgs @('-p','\.wad$','-f','zh_CN')
  pwsh scripts/bench-download.ps1 -Manifest .\game.manifest -PlanOnly -CliArgs @('-full-bundle-threshold','0.7')
#>
param(
    [Parameter(Mandatory)] [string]$Manifest,
    [int]$Rounds = 3,
    [string]$Exe = ".\manifest-cli.exe",
    [string]$OutDir = ".\.bench-out",
    [string[]]$CliArgs = @(),
    [switch]$PlanOnly
)
$ErrorActionPreference = 'Stop'
if (-not (Test-Path $Exe)) { throw "找不到 $Exe，请先运行 scripts/build.ps1" }
if ($PlanOnly) {
    & $Exe $Manifest -plan-only @CliArgs
    exit $LASTEXITCODE
}
$results = @()
for ($i = 1; $i -le $Rounds; $i++) {
    if (Test-Path $OutDir) { Remove-Item -Recurse -Force $OutDir }  # 含 .rman 存档，保证每轮全新下载
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & $Exe $Manifest -o $OutDir -s @CliArgs
    $sw.Stop()
    $results += [pscustomobject]@{ Round = $i; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 2); ExitCode = $LASTEXITCODE }
}
$results | Format-Table -AutoSize
$ok = @($results | Where-Object ExitCode -eq 0)
if ($ok.Count -gt 0) {
    $avg = [math]::Round(($ok | Measure-Object -Property Seconds -Average).Average, 2)
    Write-Host "成功轮平均耗时: ${avg}s（$($ok.Count)/$Rounds 轮成功）"
} else {
    Write-Warning "全部轮次失败"
    exit 1
}
