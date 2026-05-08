$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$ProjectDir = Split-Path -Parent $ScriptDir
$TaskName = "ImageEditBot"
$UserDir = Join-Path $ProjectDir ".用户数据"
$LogDir = Join-Path $UserDir "logs"

function Ensure-LogDir {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

function Ensure-Config {
    $envFile = Join-Path $ProjectDir ".env"
    $legacySecrets = Join-Path $ProjectDir "后台处理\bot_secrets.py"
    if (-not (Test-Path $envFile) -and -not (Test-Path $legacySecrets)) {
        Write-Host "还没有配置 Bot Token。"
        Write-Host "请先双击 Win11\配置.bat，按提示填写 Telegram Bot Token。DeepSeek API Key 为可选项。"
        exit 1
    }
}

function Invoke-Python {
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$Arguments
    )

    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        & $python.Source @Arguments
        return
    }

    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        & $py.Source -3 @Arguments
        return
    }

    throw "未找到 Python。请先安装 Python 3，并勾选 Add Python to PATH。"
}

function Install-Dependencies {
    $requirements = Join-Path $ProjectDir "后台处理\requirements.txt"
    Write-Host "安装/检查依赖..."
    Invoke-Python -Arguments @("-m", "pip", "install", "-q", "-r", $requirements)
}

function Get-BotProcesses {
    $projectPattern = [Regex]::Escape($ProjectDir)
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -match "bot\.py" -and
            $_.CommandLine -match $projectPattern
        }
}

function Stop-BotProcess {
    Get-BotProcesses | ForEach-Object {
        try {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        } catch {}
    }
}

function Test-BotTask {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    return $null -ne $task
}

function Register-BotTask {
    Ensure-LogDir
    $runner = Join-Path $ScriptDir "run_bot.ps1"
    $arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runner`""
    $action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $arguments -WorkingDirectory $ProjectDir
    $trigger = New-ScheduledTaskTrigger -AtLogOn
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -RestartCount 3 `
        -RestartInterval (New-TimeSpan -Minutes 1)

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Description "ImageEdit Bot background service" `
        -Force | Out-Null
}

function Start-BotTask {
    Start-ScheduledTask -TaskName $TaskName
}

function Stop-BotTask {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Stop-BotProcess
}

function Remove-BotTask {
    Stop-BotTask
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
}

function Clear-BotData {
    New-Item -ItemType Directory -Force -Path $UserDir | Out-Null

    $paths = @(
        "User_data.json",
        "User_log.csv",
        "User_delete.json",
        ".session_images",
        ".session_videos",
        ".pending_presets",
        ".生成图存档",
        "生成图存档",
        "User_presets",
        "User_voices",
        "logs"
    )

    foreach ($relative in $paths) {
        $path = Join-Path $UserDir $relative
        if (Test-Path $path) {
            Remove-Item -Recurse -Force $path
        }
    }

    New-Item -ItemType Directory -Force -Path (Join-Path $UserDir "User_presets") | Out-Null
    New-Item -ItemType Directory -Force -Path (Join-Path $UserDir "User_voices") | Out-Null
    Ensure-LogDir
    '{"users": {}}' | Set-Content -Path (Join-Path $UserDir "User_data.json") -Encoding UTF8
}
