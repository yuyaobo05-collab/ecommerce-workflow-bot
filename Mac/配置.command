#!/bin/bash
# 生成本地 .env 配置文件。真实 token 不会进入 Git。

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

echo ""
echo "================================================"
echo "  工作流 Bot 本地配置"
echo "================================================"
echo ""

read -r -p "请输入 Telegram Bot Token：" TG_TOKEN
if [ -z "$TG_TOKEN" ]; then
    echo "❌ Telegram Bot Token 不能为空。"
    exit 1
fi

read -r -p "请输入 DeepSeek API Key：" DS_API_KEY
if [ -z "$DS_API_KEY" ]; then
    echo "❌ DeepSeek API Key 不能为空。"
    exit 1
fi

umask 077
cat > "$ENV_FILE" <<EOF
# 本地运行配置。此文件包含敏感信息，不会被 Git 提交。

TG_TOKEN=$TG_TOKEN
DS_API_KEY=$DS_API_KEY
EOF

chmod 600 "$ENV_FILE"

echo ""
echo "✅ 已写入：$ENV_FILE"
echo "之后可以双击「安装.command」或「重启.command」启动服务。"
