. "$PSScriptRoot\_common.ps1"

Ensure-Config
Install-Dependencies

if (-not (Test-BotTask)) {
    Register-BotTask
}

Stop-BotTask
Start-BotTask

Write-Host "后台任务已重启。"
Write-Host "日志目录：$LogDir"
