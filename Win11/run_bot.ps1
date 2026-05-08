$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $PSScriptRoot
$UserDataDirName = "." + [string]([char]0x7528) + [string]([char]0x6237) + [string]([char]0x6570) + [string]([char]0x636E)
$LogDir = Join-Path (Join-Path $ProjectDir $UserDataDirName) "logs"
$OutLog = Join-Path $LogDir "windows.out.log"
$ErrLog = Join-Path $LogDir "windows.err.log"
$BotFile = Join-Path $ProjectDir "bot.py"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location -LiteralPath $ProjectDir

$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) {
    & $python.Source $BotFile >> $OutLog 2>> $ErrLog
    exit $LASTEXITCODE
}

$py = Get-Command py -ErrorAction SilentlyContinue
if ($py) {
    & $py.Source -3 $BotFile >> $OutLog 2>> $ErrLog
    exit $LASTEXITCODE
}

"Python was not found. Install Python 3 and enable Add Python to PATH." | Out-File -FilePath $ErrLog -Append -Encoding utf8
exit 1
