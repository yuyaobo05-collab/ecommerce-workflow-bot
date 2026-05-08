$ErrorActionPreference = "Stop"

$ScriptDir = $PSScriptRoot
$WinDir = Split-Path -Parent $ScriptDir
$ProjectDir = Split-Path -Parent $WinDir
$TaskName = "ImageEditBot"
$BackendDirName = [string]([char]0x540E) + [string]([char]0x53F0) + [string]([char]0x5904) + [string]([char]0x7406)
$UserDataDirName = "." + [string]([char]0x7528) + [string]([char]0x6237) + [string]([char]0x6570) + [string]([char]0x636E)
$ResultArchiveDirName = [string]([char]0x751F) + [string]([char]0x6210) + [string]([char]0x56FE) + [string]([char]0x5B58) + [string]([char]0x6863)
$BackendDir = Join-Path $ProjectDir $BackendDirName
$UserDir = Join-Path $ProjectDir $UserDataDirName
$LogDir = Join-Path $UserDir "logs"

function Ensure-LogDir {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
}

function Ensure-Config {
    $envFile = Join-Path $ProjectDir ".env"
    $legacySecrets = Join-Path $BackendDir "bot_secrets.py"
    if (-not (Test-Path $envFile) -and -not (Test-Path $legacySecrets)) {
        Write-Host "Bot token is not configured."
        Write-Host "Run the config .bat file in the Win11 folder first. DeepSeek API key is optional."
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

    throw "Python was not found. Install Python 3 and enable Add Python to PATH."
}

function Install-Dependencies {
    $requirements = Join-Path $BackendDir "requirements.txt"
    Write-Host "Installing/checking dependencies..."
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
        "." + $ResultArchiveDirName,
        $ResultArchiveDirName,
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
