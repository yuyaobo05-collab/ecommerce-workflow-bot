. "$PSScriptRoot\_common.ps1"

Write-Host ""
Write-Host "================================================"
Write-Host "  RunningHubBot 数据重置"
Write-Host "  此操作将清除所有用户数据，包括 API Key、日志、语音和图片预设。"
Write-Host "  不会删除 .env 里的 Bot Token。"
Write-Host "================================================"
Write-Host ""

$answer = Read-Host "确认清空全部数据？输入 yes 继续，其他取消"
if ($answer -ne "yes") {
    Write-Host "已取消，数据未动。"
    exit 0
}

$wasInstalled = Test-BotTask
if ($wasInstalled) {
    Write-Host "正在停止后台任务..."
    Stop-BotTask
} else {
    Stop-BotProcess
}

Write-Host "正在清空数据..."
Clear-BotData
Write-Host "数据已全部清空。"

if ($wasInstalled) {
    Start-BotTask
    Write-Host "后台任务已重启。"
} else {
    Write-Host "尚未安装后台任务。需要运行时，请双击 Win11\启动.bat。"
}
