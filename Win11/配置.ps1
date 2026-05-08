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

$dsApiKey = Read-Host "请输入 DeepSeek API Key（可选，仅 AI 随机风格需要，直接回车可跳过）"

$content = @"
# 本地运行配置。此文件包含敏感信息，不会被 Git 提交。

TG_TOKEN=$tgToken
DS_API_KEY=$dsApiKey
"@

$content | Set-Content -Path $EnvFile -Encoding UTF8
Write-Host ""
Write-Host "已写入：$EnvFile"
if ([string]::IsNullOrWhiteSpace($dsApiKey)) {
    Write-Host "未填写 DeepSeek API Key，AI 随机风格功能将不可用，其他工作流不受影响。"
}
Write-Host "之后可以双击 Win11\启动.bat 启动后台任务。"
