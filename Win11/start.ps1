. "$PSScriptRoot\_common.ps1"

Ensure-Config
Install-Dependencies
Register-BotTask
Stop-BotProcess
Start-BotTask

Write-Host "Background task was created and started."
Write-Host "Log directory: $LogDir"
