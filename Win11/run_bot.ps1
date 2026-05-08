$ErrorActionPreference = "Stop"

$ProjectDir = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $ProjectDir ".用户数据\logs"
$OutLog = Join-Path $LogDir "windows.out.log"
$ErrLog = Join-Path $LogDir "windows.err.log"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location -LiteralPath $ProjectDir

$python = Get-Command python -ErrorAction SilentlyContinue
if ($python) {
    & $python.Source bot.py >> $OutLog 2>> $ErrLog
    exit $LASTEXITCODE
}

$py = Get-Command py -ErrorAction SilentlyContinue
if ($py) {
    & $py.Source -3 bot.py >> $OutLog 2>> $ErrLog
    exit $LASTEXITCODE
}

"未找到 Python。请先安装 Python 3，并勾选 Add Python to PATH。" | Out-File -FilePath $ErrLog -Append -Encoding utf8
exit 1
