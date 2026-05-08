#!/bin/bash
# 卸载 ImageEdit Bot 的 macOS LaunchAgent。

set -e

LABEL="com.imageedit.bot"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
rm -f "$PLIST"

echo "✅ 后台服务已卸载。"
