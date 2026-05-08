. "$PSScriptRoot\_common.ps1"

Write-Host ""
Write-Host "================================================"
Write-Host "  ImageEdit Bot data reset"
Write-Host "  This will remove user data, logs, voice presets, and image presets."
Write-Host "  It will not remove the bot token in .env."
Write-Host "================================================"
Write-Host ""

$answer = Read-Host "Type yes to continue"
if ($answer -ne "yes") {
    Write-Host "Canceled. Data was not changed."
    exit 0
}

$wasInstalled = Test-BotTask
if ($wasInstalled) {
    Write-Host "Stopping background task..."
    Stop-BotTask
} else {
    Stop-BotProcess
}

Write-Host "Clearing data..."
Clear-BotData
Write-Host "Data was cleared."

if ($wasInstalled) {
    Start-BotTask
    Write-Host "Background task was restarted."
} else {
    Write-Host "Background task is not installed. Run the start .bat file in the Win11 folder when needed."
}
