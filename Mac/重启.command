#!/bin/bash
# 重启已安装的 macOS LaunchAgent。代码更新后双击这个即可生效。

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.imageedit.bot"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_DIR="$SCRIPT_DIR/.用户数据/logs"

mkdir -p "$LOG_DIR"

if [ ! -f "$SCRIPT_DIR/.env" ] && [ ! -f "$SCRIPT_DIR/后台处理/bot_secrets.py" ]; then
    echo "⚠️ 还没有配置 Bot Token。"
    echo "请先双击「配置.command」，按提示填写 Telegram Bot Token。DeepSeek API Key 为可选项。"
    exit 1
fi

if [ ! -f "$PLIST" ]; then
    echo "⚠️ 还没有安装后台服务，请先双击「安装.command」。"
    exit 1
fi

echo "📦 安装/检查依赖..."
pip3 install -q -r "$SCRIPT_DIR/后台处理/requirements.txt"

launchctl bootstrap "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
launchctl enable "gui/$(id -u)/$LABEL"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "✅ 后台服务已重启。"
echo "日志：$LOG_DIR/bot.log"
