. "$PSScriptRoot\_common.ps1"

Ensure-Config
Install-Dependencies

if (-not (Test-BotTask)) {
    Register-BotTask
}

Stop-BotTask
Start-BotTask

Write-Host "Background task was restarted."
Write-Host "Log directory: $LogDir"
