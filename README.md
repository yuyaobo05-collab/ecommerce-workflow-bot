# 电商工作流 Bot

这是一个面向电商场景的 Telegram 工作流 Bot，用来调用 RunningHub 上的图片、视频和音频工作流。

适合把商品图、人像模特、服装参考、视频素材和声音样本接入自动化处理流程，例如商品图编辑、换装试穿、扩图、动图、视频换衣、首尾帧视频、场景替换和语音生成等电商内容生产任务。

## 内容合规声明

本项目仅提供电商内容工作流的技术调用工具，禁止使用本项目生成、编辑、传播色情、露骨性内容，以及任何违法违规、侵权或未经授权的内容。

使用者应自行确保上传素材、提示词、生成结果和后续用途合法合规。任何用户生成内容、使用方式和传播行为均由使用者自行负责，与本项目开源作者和维护者无关。

本项目为个人兴趣开源项目，纯属为爱发电，不收取任何费用，也不提供商业服务承诺。

## 配置

开源仓库里不会保存任何真实 Token。首次运行前，用下面任意一种方式生成本地 `.env` 文件：

- macOS：双击 `Mac/配置.command`
- Windows 11：双击 `Win11/配置.bat`
- 手动配置：复制 `.env.example` 为 `.env`，然后填写：

```env
TG_TOKEN=你的 Telegram Bot Token

# 可选：仅 /aiprompt 和图片面板里的“AI 随机风格”需要
DS_API_KEY=你的 DeepSeek API Key
```

`TG_TOKEN` 是必填项。`DS_API_KEY` 不是必填项；不填写时，AI 随机风格功能不可用，其他电商工作流不受影响。

`.env`、`.用户数据/`、日志、用户语音和旧版 `后台处理/bot_secrets.py` 都会被 Git 忽略。

## Bot 命令菜单

在 BotFather 里使用 `/setcommands` 时，可以直接复制下面这段：

```text
start - 帮助菜单 / 当前状态
key - 设置 RunningHub API Key
aiprompt - AI 生成随机风格提示词
save - 保存文字预设
saveimg - 保存图片预设
savevoice - 保存声音角色
voice - 选择声音角色
del - 删除预设
presetflow - 切换换衣工作流
expand - 图片扩展开关
gifsec - 切换动图/首尾时长
talkprompt - 说话视频提示词
flprompt - 首尾默认提示词
comparetext - 设置对比图文案
compareswitch - 对比图功能开关
reset - 清空所有设置
```

## macOS 后台运行

双击 `Mac/安装.command` 安装并启动后台服务。
之后代码更新或配置修改后，双击 `Mac/重启.command`。
不再使用时，双击 `Mac/卸载.command`。
需要清空用户数据时，双击 `Mac/重置.command`。

## Windows 11 后台运行

双击 `Win11/启动.bat` 会创建并启动一个 Windows 任务计划，登录后自动启动 Bot。
代码更新或配置修改后，双击 `Win11/重启.bat`。
不再使用时，双击 `Win11/卸载.bat`。
需要清空用户数据时，双击 `Win11/重置.bat`。

## 测试

```bash
python3 -m pytest 后台处理/tests
```
