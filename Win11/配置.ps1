$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $ProjectDir ".env"

Write-Host ""
Write-Host "================================================"
Write-Host "  工作流 Bot 本地配置"
Write-Host "================================================"
Write-Host ""

$tgToken = Read-Host "请输入 Telegram Bot Token"
if ([string]::IsNullOrWhiteSpace($tgToken)) {
    Write-Host "Telegram Bot Token 不能为空。"
    exit 1
}

$dsApiKey = Read-Host "请输入 DeepSeek API Key"
if ([string]::IsNullOrWhiteSpace($dsApiKey)) {
    Write-Host "DeepSeek API Key 不能为空。"
    exit 1
}

$content = @"
# 本地运行配置。此文件包含敏感信息，不会被 Git 提交。

TG_TOKEN=$tgToken
DS_API_KEY=$dsApiKey
"@

$content | Set-Content -Path $EnvFile -Encoding UTF8
Write-Host ""
Write-Host "已写入：$EnvFile"
Write-Host "之后可以双击 Win11\启动.bat 启动后台任务。"
