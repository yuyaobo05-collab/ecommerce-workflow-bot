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

read -r -p "请输入 DeepSeek API Key（可选，仅 AI 随机风格需要，直接回车可跳过）：" DS_API_KEY

umask 077
cat > "$ENV_FILE" <<EOF
# 本地运行配置。此文件包含敏感信息，不会被 Git 提交。

TG_TOKEN=$TG_TOKEN
DS_API_KEY=$DS_API_KEY
EOF

chmod 600 "$ENV_FILE"

echo ""
echo "✅ 已写入：$ENV_FILE"
if [ -z "$DS_API_KEY" ]; then
    echo "ℹ️ 未填写 DeepSeek API Key，AI 随机风格功能将不可用，其他工作流不受影响。"
fi
echo "之后可以双击「安装.command」或「重启.command」启动服务。"
