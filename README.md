# 工作流 Bot

这是一个 Telegram 前端 Bot，用来调用 RunningHub 上的图片、视频和音频工作流。

## 配置

开源仓库里不会保存任何真实 Token。首次运行前，用下面任意一种方式生成本地 `.env` 文件：

- macOS：双击 `Mac/配置.command`
- Windows 11：双击 `Win11/配置.bat`
- 手动配置：复制 `.env.example` 为 `.env`，然后填写：

```env
TG_TOKEN=你的 Telegram Bot Token
DS_API_KEY=你的 DeepSeek API Key
```

`.env`、`.用户数据/`、日志、用户语音和旧版 `后台处理/bot_secrets.py` 都会被 Git 忽略。

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
