. "$PSScriptRoot\_common.ps1"

Ensure-Config
Install-Dependencies
Register-BotTask
Stop-BotProcess
Start-BotTask

Write-Host "后台任务已创建并启动。"
Write-Host "日志目录：$LogDir"
