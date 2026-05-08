#!/bin/bash
# 安装为 macOS LaunchAgent：登录后自动启动，异常退出自动拉起。

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.imageedit.bot"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$SCRIPT_DIR/.用户数据/logs"

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

if [ ! -f "$SCRIPT_DIR/.env" ] && [ ! -f "$SCRIPT_DIR/后台处理/bot_secrets.py" ]; then
    echo "⚠️ 还没有配置 Bot Token。"
    echo "请先双击「配置.command」，按提示填写 Telegram Bot Token 和 DeepSeek API Key。"
    exit 1
fi

echo "📦 安装/检查依赖..."
pip3 install -q -r "$SCRIPT_DIR/后台处理/requirements.txt"

export SCRIPT_DIR PLIST LOG_DIR LABEL
python3 - <<'PY'
import os
import plistlib

script_dir = os.environ["SCRIPT_DIR"]
log_dir = os.environ["LOG_DIR"]
plist_path = os.environ["PLIST"]
label = os.environ["LABEL"]

plist = {
    "Label": label,
    "ProgramArguments": [
        "/usr/bin/caffeinate",
        "-dimsu",
        "/usr/bin/python3",
        "bot.py",
    ],
    "WorkingDirectory": script_dir,
    "EnvironmentVariables": {
        "PYTHONPYCACHEPREFIX": os.path.join(script_dir, "后台处理", "__pycache__"),
    },
    "RunAtLoad": True,
    "KeepAlive": True,
    "StandardOutPath": os.path.join(log_dir, "launchd.out.log"),
    "StandardErrorPath": os.path.join(log_dir, "launchd.err.log"),
}

with open(plist_path, "wb") as f:
    plistlib.dump(plist, f)
PY

launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/$LABEL"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "✅ 后台服务已安装并启动。"
echo "日志：$LOG_DIR/bot.log"
