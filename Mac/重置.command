#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
USER_DIR="$SCRIPT_DIR/.用户数据"
LABEL="com.imageedit.bot"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

cleanup() {
    return 0
}

trap cleanup EXIT INT TERM HUP

echo ""
echo "================================================"
echo "  ⚠️  RunningHubBot 数据重置"
echo "  此操作将清除所有用户数据，包括 API Key"
echo "  和图片预设，不可恢复！"
echo "================================================"
echo ""
read -p "确认清空全部数据？输入 yes 继续，其他取消：" answer

if [ "$answer" != "yes" ]; then
    echo ""
    echo "❌ 已取消，数据未动。"
    echo ""
    exit 0
fi

echo ""
echo "正在清空数据..."

SERVICE_INSTALLED=0
if [ -f "$PLIST" ]; then
    SERVICE_INSTALLED=1
    echo "正在停止后台服务..."
    launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
fi

echo "正在停止残留 bot.py 进程..."
pkill -TERM -f "python.*bot.py" 2>/dev/null || true
for _ in 1 2 3 4 5; do
    if ! pgrep -f "python.*bot.py" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
pkill -KILL -f "python.*bot.py" 2>/dev/null || true

# 清空 User_data.json
mkdir -p "$USER_DIR"
echo '{"users": {}}' > "$USER_DIR/User_data.json"

# 删除日志和删除队列
rm -f "$USER_DIR/User_log.csv"
rm -f "$USER_DIR/User_delete.json"
rm -rf "$USER_DIR/.session_images"
rm -rf "$USER_DIR/.session_videos"
rm -rf "$USER_DIR/.pending_presets"

# 清空媒体预设文件夹
rm -rf "$USER_DIR/User_presets"
rm -rf "$USER_DIR/User_voices"
mkdir -p "$USER_DIR/User_presets"
mkdir -p "$USER_DIR/User_voices"

echo ""
echo "✅ 数据已全部清空。"
echo ""

if [ "$SERVICE_INSTALLED" -eq 1 ]; then
    echo "正在重启后台服务..."
    launchctl bootstrap "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
    launchctl enable "gui/$(id -u)/$LABEL"
    launchctl kickstart -k "gui/$(id -u)/$LABEL"
    echo "✅ 后台服务已重启。"
else
    echo "⚠️ 尚未安装后台服务。需要运行时，请双击「安装.command」。"
fi
