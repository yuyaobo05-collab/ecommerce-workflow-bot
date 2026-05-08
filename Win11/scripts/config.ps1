$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$WinDir = Split-Path -Parent $ScriptDir
$ProjectDir = Split-Path -Parent $WinDir
$EnvFile = Join-Path $ProjectDir ".env"

Write-Host ""
Write-Host "================================================"
Write-Host "  Ecommerce Workflow Bot config"
Write-Host "================================================"
Write-Host ""

$tgToken = Read-Host "Telegram Bot Token"
if ([string]::IsNullOrWhiteSpace($tgToken)) {
    Write-Host "Telegram Bot Token is required."
    exit 1
}

$dsApiKey = Read-Host "DeepSeek API Key (optional, press Enter to skip)"

$content = @"
# Local runtime config. This file contains secrets and is ignored by Git.

TG_TOKEN=$tgToken
DS_API_KEY=$dsApiKey
"@

$content | Set-Content -Path $EnvFile -Encoding UTF8
Write-Host ""
Write-Host "Wrote: $EnvFile"
if ([string]::IsNullOrWhiteSpace($dsApiKey)) {
    Write-Host "DeepSeek API key was skipped. AI random style will be unavailable; other workflows are unaffected."
}
Write-Host "Next, run the start .bat file in the Win11 folder."
