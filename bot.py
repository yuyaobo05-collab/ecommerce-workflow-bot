#!/usr/bin/env python3
"""
Telegram Bot — RunningHub 图像编辑工作流
功能：
  - 每用户 API Key 管理（持久化到 data.json）
  - 每用户提示词存档
  - InlineKeyboard 分步选择提示词
  - 异步并发，多用户任务互不阻塞
"""

import os
import sys
import csv
import json
import time
import math
import signal
import shutil
import mimetypes
import logging
import asyncio
import contextlib
import aiohttp
import uuid
import re
from datetime import datetime, time as dt_time, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional


PROJECT_DIR = Path(__file__).resolve().parent


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            continue
        if name in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].strip()
        os.environ[name] = value


_load_env_file(PROJECT_DIR / ".env")

# 从 后台处理/ 导入原生 duck 解码器
sys.path.insert(0, str(PROJECT_DIR / "后台处理"))
from duck_decode import decode_duck_image, decode_duck_media
from runninghub import build_node_info_list
from workflows.registry import (
    DEFAULT_CUSTOM_WORKFLOW_KEY,
    DEFAULT_PRESET_WORKFLOW_KEY,
    PRESET_WORKFLOW_KEYS,
    WorkflowSpec,
    WORKFLOWS,
)
from telegram import BotCommand, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ChatAction
from telegram.error import Conflict, RetryAfter, TimedOut
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ─────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────
try:
    import bot_secrets as _bot_secrets
except ImportError:
    _bot_secrets = None

_TG_TOKEN = getattr(_bot_secrets, "TG_TOKEN", "")
_DS_API_KEY = getattr(_bot_secrets, "DS_API_KEY", "")

TG_TOKEN = (os.getenv("TG_TOKEN") or _TG_TOKEN or "").strip()


def _secret_or_env(name: str, default: str = "") -> str:
    return (os.getenv(name) or getattr(_bot_secrets, name, "") or default or "").strip()


def _runninghub_ai_app_endpoint(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.isdigit():
        return f"https://www.runninghub.cn/openapi/v2/run/ai-app/{value}"
    return value


def configure_voice_clone_workflow():
    spec = WORKFLOWS.get("voice_clone")
    if not spec:
        return
    endpoint = _runninghub_ai_app_endpoint(_secret_or_env("RH_VOICE_CLONE_ENDPOINT", spec.endpoint))
    sample_node = spec.nodes.get("sample_audio", ("1", "audio"))
    text_node = spec.nodes.get("text", ("2", "text"))
    WORKFLOWS["voice_clone"] = WorkflowSpec(
        key=spec.key,
        label=spec.label,
        endpoint=endpoint,
        input_mode=spec.input_mode,
        node_order=spec.node_order,
        nodes={
            "sample_audio": (
                _secret_or_env("RH_VOICE_SAMPLE_NODE_ID", sample_node[0]),
                _secret_or_env("RH_VOICE_SAMPLE_FIELD", sample_node[1]),
            ),
            "text": (
                _secret_or_env("RH_VOICE_TEXT_NODE_ID", text_node[0]),
                _secret_or_env("RH_VOICE_TEXT_FIELD", text_node[1]),
            ),
        },
        fixed_values=spec.fixed_values,
    )


configure_voice_clone_workflow()

RH_UPLOAD_URL    = "https://www.runninghub.cn/openapi/v2/media/upload/binary"
RH_QUERY_URL     = "https://www.runninghub.cn/openapi/v2/query"

USER_DIR         = PROJECT_DIR / ".用户数据"
LOG_DIR          = USER_DIR / "logs"
BOT_LOG_FILE     = LOG_DIR / "bot.log"
SESSION_IMAGE_DIR = USER_DIR / ".session_images"
SESSION_VIDEO_DIR = USER_DIR / ".session_videos"
PENDING_PRESET_DIR = USER_DIR / ".pending_presets"
PRESET_IMAGE_DIR = USER_DIR / "User_presets"
VOICE_PRESET_DIR = USER_DIR / "User_voices"
LEGACY_RESULT_ARCHIVE_DIR = USER_DIR / "生成图存档"
RESULT_ARCHIVE_DIR = USER_DIR / ".生成图存档"

POLL_INTERVAL     = 1    # 秒
POLL_TIMEOUT      = 20 * 60  # 最多等 20 分钟
MEDIA_TRANSFER_TIMEOUT = 20 * 60
MAX_PROMPTS_SHOWN = 8    # InlineKeyboard 最多显示几个提示词按钮
IMAGE_EXTEND_PIXELS = 200
IMAGE_EXTEND_DIRECTIONS = ("top", "bottom", "right", "left")
IMAGE_EXTEND_LABELS = {
    "top": "上",
    "bottom": "下",
    "right": "右",
    "left": "左",
}
DEFAULT_ANIMATION_PROMPT = ""
DEFAULT_ANIMATION_SECONDS = 5
ANIMATION_SECONDS_OPTIONS = (5, 10)
FIRST_LAST_VIDEO_SECONDS = 5
FIRST_LAST_VIDEO_FALLBACK_MAX_SIDE = 832
TALKING_VIDEO_MAX_SIDE = 960
TALKING_VIDEO_FPS = 24
TALKING_VIDEO_DEFAULT_SECONDS = 10
DEFAULT_TALKING_VIDEO_PROMPT = (
    "subtle lip sync, natural mouth movement, minimal jaw movement, "
    "relaxed facial muscles, soft articulation, conversational tone,"
    "no exaggerated mouth shapes, no over-enunciation, no wide open mouth, "
    "no facial strain, no teeth gnashing,calm expression, micro-expressions only"
)
DEFAULT_FIRST_LAST_VIDEO_PROMPT = (
    "根据提供的参考画面生成一段自然流畅的视频。\n"
    "视频从起始画面的状态开始，逐渐过渡到最终画面的状态。整个过程要连贯、真实、符合物理逻辑，不能出现突然跳变或不自然的变化。\n"
    "全程严格保持主体身份一致，包括脸部特征、发型、体型比例、服装、配饰、材质、光照风格和整体画面质感。\n"
    "主体需要从起始状态中的姿势、表情、构图和环境，自然移动到最终状态中的姿势、表情、构图和环境。所有变化都应该是渐进、稳定、合理的。\n"
    "镜头保持稳定、有电影感，只允许轻微且平滑的镜头运动。保持透视、比例、光照、背景结构、色调和画质一致。\n"
    "避免身份漂移、脸部变形、身体扭曲、多余肢体、重复身体、手部崩坏、眼神不稳定、闪烁、衣服融化、背景坍塌、光影不一致、姿势突然跳变或动作不自然。\n"
    "最终效果应像一个连续拍摄的电影镜头，细节清晰，构图稳定，动作真实，从开始到结束平滑过渡。"
)

DS_API_KEY         = (os.getenv("DS_API_KEY") or _DS_API_KEY or "").strip()
DS_API_URL         = "https://api.deepseek.com/v1/chat/completions"
DS_PROMPT_FILE     = PROJECT_DIR / "后台处理" / "deepseek_prompt.txt"
DS_PROMPT_FALLBACK = "你是一个图像处理提示词专家，请生成一段简洁有创意的中文提示词，不超过50字，直接输出内容。"
DS_MODEL           = "deepseek-v4-pro"
DS_MAX_TOKENS      = 1000
DS_GENERATE_RETRIES = 3
DS_PROMPT_MIN_CHARS = 20
DS_PROMPT_MAX_CHARS = 900
DS_GENERATE_USER_MESSAGE = (
    "请生成一个完整提示词。要求：必须非空；只输出提示词正文；不要编号、不要解释、不要省略号；"
    "长度控制在300到600个中文字符内；最后用句号、感叹号或问号自然收尾。"
)


def _load_ds_system_prompt() -> str:
    try:
        text = DS_PROMPT_FILE.read_text(encoding="utf-8").strip()
        return text or DS_PROMPT_FALLBACK
    except Exception:
        return DS_PROMPT_FALLBACK


def _coerce_ds_content_to_text(content: Any) -> str:
    """兼容字符串与少数 OpenAI-compatible 的分段 content 结构。"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
        return "\n".join(parts)
    return ""


def _normalize_ds_prompt(text: str) -> str:
    text = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:\w+)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text).strip()
    text = re.sub(r"^(?:提示词|预设内容|生成提示词)\s*[:：]\s*", "", text).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _looks_incomplete_ds_prompt(prompt: str) -> bool:
    if not prompt:
        return True
    if not prompt.endswith(("。", "！", "？", ".", "!", "?")):
        return True
    if prompt.endswith(("…", "……", "...")):
        return True
    if prompt.endswith(("，", "、", "；", "：", ":", ",", ";", "-", "—", "（", "(", "【", "「", "“")):
        return True
    return any(
        prompt.count(left) > prompt.count(right)
        for left, right in (("（", "）"), ("(", ")"), ("【", "】"), ("「", "」"), ("“", "”"))
    )


def _extract_ds_prompt_from_response(data: dict) -> str:
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("DeepSeek 没有返回 choices")

    choice = choices[0]
    if not isinstance(choice, dict):
        raise RuntimeError("DeepSeek choices 格式异常")

    finish_reason = choice.get("finish_reason")
    if finish_reason == "length":
        raise RuntimeError("DeepSeek 返回被 max_tokens 截断")
    if finish_reason and finish_reason not in {"stop"}:
        raise RuntimeError(f"DeepSeek 返回结束原因异常：{finish_reason}")

    message = choice.get("message") or {}
    if not isinstance(message, dict):
        raise RuntimeError("DeepSeek message 格式异常")

    generated = _normalize_ds_prompt(_coerce_ds_content_to_text(message.get("content")))
    if len(generated) < DS_PROMPT_MIN_CHARS:
        raise RuntimeError("DeepSeek 返回空内容或内容过短")
    if len(generated) > DS_PROMPT_MAX_CHARS:
        raise RuntimeError(f"DeepSeek 返回过长（{len(generated)} 字）")
    if _looks_incomplete_ds_prompt(generated):
        raise RuntimeError("DeepSeek 返回看起来未完整收尾")
    return generated


# 启动时读一次缓存到内存；要改提示词重启 bot 即可生效。
DS_SYSTEM_PROMPT = _load_ds_system_prompt()
DS_HISTORY_MAX     = 6   # 保留最近几轮，防止 token 爆炸

DATA_FILE     = USER_DIR / "User_data.json"
LOG_FILE      = USER_DIR / "User_log.csv"
CLEANUP_FILE  = USER_DIR / "User_delete.json"
_data_lock    = asyncio.Lock()
_log_lock     = asyncio.Lock()
_cleanup_lock = asyncio.Lock()
_http_lock    = asyncio.Lock()
WARNING_DELETE_SECONDS  = 5        # 即时反馈消息（成功/失败/警告）5 秒后自动删除
WAITING_FLOW_TIMEOUT_SECONDS = 5 * 60  # 需要用户继续输入的流程，5 分钟无操作后自动失效
TG_CAPTION_MAX          = 1024     # Telegram caption 字符上限
MAX_ACTIVE_TASKS_PER_USER = 10     # 每个用户最多同时跑几个 RunningHub 处理任务
TG_CONNECTION_POOL_SIZE   = 128    # Bot API 普通请求连接池，避免多任务回传时 Pool timeout
TG_GET_UPDATES_POOL_SIZE  = 8      # long-poll 单独连接池
TG_POOL_TIMEOUT           = 30.0
TG_CONNECT_TIMEOUT        = 10.0
TG_READ_TIMEOUT           = 30.0
TG_WRITE_TIMEOUT          = 60.0
TG_GET_UPDATES_READ_TIMEOUT = 65.0
FFMPEG_AUDIO_EXTRACT_TIMEOUT = 120

# 共享 Session 的默认超时：覆盖所有未单独传 timeout 的请求。
# 视频上传/下载可能较慢，统一给媒体传输 20 分钟；connect 仍快速失败。
SHARED_HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=MEDIA_TRANSFER_TIMEOUT,
    connect=10,
    sock_connect=10,
    sock_read=MEDIA_TRANSFER_TIMEOUT,
)
shared_http_session: Optional[aiohttp.ClientSession] = None
_status_edit_blocked_until: dict[int, float] = {}  # chat_id -> blocked_until

IMAGE_MIME_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}
DEFAULT_IMAGE_CONTENT_TYPE = "image/jpeg"
VIDEO_MIME_EXTENSIONS = {
    "video/mp4": ".mp4",
    "video/quicktime": ".mov",
    "video/webm": ".webm",
    "video/mpeg": ".mpeg",
    "video/x-m4v": ".m4v",
    "video/x-matroska": ".mkv",
    "video/x-msvideo": ".avi",
}
DEFAULT_VIDEO_CONTENT_TYPE = "video/mp4"
AUDIO_MIME_EXTENSIONS = {
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/wave": ".wav",
    "audio/ogg": ".ogg",
    "application/ogg": ".ogg",
    "video/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/aac": ".aac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "audio/webm": ".webm",
    "video/webm": ".webm",
    "audio/flac": ".flac",
}
DEFAULT_AUDIO_CONTENT_TYPE = "audio/mpeg"
VOICE_TEXT_MAX_CHARS = 1000
VOICE_RESULT_SUFFIXES = {".mp3", ".wav", ".ogg", ".opus", ".m4a", ".aac", ".flac", ".webm"}
DOCUMENT_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}


def seconds_until_midnight() -> int:
    """返回距离下一个北京时间凌晨 0:00 的秒数（最少 60 秒）。"""
    cst = timezone(timedelta(hours=8))
    now = datetime.now(tz=cst)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(int((midnight - now).total_seconds()), 60)

# ─────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
_log_format = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_stream_handler = logging.StreamHandler()
_stream_handler.setFormatter(_log_format)
_file_handler = RotatingFileHandler(
    BOT_LOG_FILE,
    maxBytes=2_000_000,
    backupCount=5,
    encoding="utf-8",
)
_file_handler.setFormatter(_log_format)
logging.basicConfig(
    level=logging.INFO,
    handlers=[_stream_handler, _file_handler],
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def validate_runtime_config():
    if not TG_TOKEN:
        raise RuntimeError("请在 .env、bot_secrets.py 或环境变量 TG_TOKEN 中配置 Telegram Bot Token。")


def migrate_result_archive_dir():
    """将旧的可见生成图存档目录迁移到隐藏目录。"""
    try:
        if not LEGACY_RESULT_ARCHIVE_DIR.exists() or LEGACY_RESULT_ARCHIVE_DIR == RESULT_ARCHIVE_DIR:
            return
        RESULT_ARCHIVE_DIR.parent.mkdir(parents=True, exist_ok=True)
        if not RESULT_ARCHIVE_DIR.exists():
            LEGACY_RESULT_ARCHIVE_DIR.rename(RESULT_ARCHIVE_DIR)
            logger.info("生成图存档已迁移到隐藏目录：%s", RESULT_ARCHIVE_DIR)
            return
        for item in LEGACY_RESULT_ARCHIVE_DIR.iterdir():
            target = RESULT_ARCHIVE_DIR / item.name
            if target.exists():
                target = RESULT_ARCHIVE_DIR / f"{item.stem}_{datetime.now().strftime('%H%M%S%f')}{item.suffix}"
            item.rename(target)
        LEGACY_RESULT_ARCHIVE_DIR.rmdir()
        logger.info("旧生成图存档已合并到隐藏目录：%s", RESULT_ARCHIVE_DIR)
    except Exception:
        logger.exception("生成图存档迁移失败")

# 禁用提示词（新预设 / 自定义 / caption）
BANNED_PROMPT_MESSAGES = {
    "幼女": "提示词包含禁用词：幼女",
    "萝莉": "提示词包含禁用词：萝莉",
    "小学生": "提示词包含禁用词：小学生",
    "未成年": "提示词包含禁用词：未成年",
    "儿童": "提示词包含禁用词：儿童",
    "loli": "提示词包含禁用词：loli",
}


# 用户流程状态：每个用户同时只允许一个等待输入流程，避免多张图互相抢状态
user_states: dict[int, dict] = {}
user_state_locks: dict[int, asyncio.Lock] = {}
# 暂存待处理的提示词会话：{session_id: {"user_id": int, "image_path": str, "prompt_items": [(name, content), ...]}}
pending_prompt_sessions: dict[str, dict] = {}
# 后台任务，防止被过早回收
active_tasks: set[asyncio.Task] = set()
user_active_task_counts: dict[int, int] = {}
_user_task_count_lock = asyncio.Lock()
# DeepSeek 每用户对话历史（当天，凌晨清零）
ds_histories: dict[int, list] = {}
# 待保存图片预设：{save_id: {"path": str, "user_id": int, "chat_id": int}}
pending_preset_saves: dict[str, dict] = {}
# DS 每次生成的独立暂存：{ds_sid: {"prompt": str, "session_id": str, "user_id": int}}
ds_pending: dict[str, dict] = {}
# 视频换衣暂存：{session_id: {"user_id": int, "video_path": str, ...}}
pending_video_outfit_sessions: dict[str, dict] = {}
# 视频换衣结果源文件：{token: {"path": str, "filename": str, "user_id": int, ...}}
pending_video_outfit_sources: dict[str, dict] = {}
# 说话视频音频来源：{token: {"path": str, "filename": str, "duration_seconds": int, ...}}
pending_talking_video_audios: dict[str, dict] = {}
# presetflow 命令暂存：{user_id: {"cmd_msg_id": int, "chat_id": int}}
presetflow_pending: dict[int, dict] = {}
# voice 面板暂存：{"chat_id:panel_msg_id": {"cmd_msg_id": int, "chat_id": int, "user_id": int}}
voice_panel_pending: dict[str, dict] = {}


WAITING_STATE_LABELS = {
    "WAITING_CUSTOM_PROMPT": "自定义提示词",
    "WAITING_ANIMATION_PROMPT": "生成动图提示词",
    "WAITING_FACE_IMAGE": "参考换脸",
    "WAITING_BODY_REF_IMAGE": "参考换衣",
    "WAITING_SCENE_PERSON_IMAGE": "场景换人",
    "WAITING_VIDEO_OUTFIT_IMAGE": "视频换衣",
    "WAITING_LAST_FRAME_IMAGE": "首尾视频",
    "WAITING_FIRST_LAST_PROMPT": "首尾视频提示词",
    "WAITING_QWEN_BODY_REF_IMAGE": "参考换衣",
    "WAITING_PRESET_NAME": "保存图片预设",
    "WAITING_IMAGE_PRESET_IMAGE": "保存图片预设",
    "WAITING_VOICE_SAMPLE": "保存声音角色",
    "WAITING_VOICE_TEXT": "声音克隆文案",
    "WAITING_TALKING_VIDEO_AUDIO": "说话视频音频",
    "WAITING_TALKING_VIDEO_IMAGE": "说话视频图片",
    "WAITING_COMPARE_ORIG": "对比图原图文案",
    "WAITING_COMPARE_RESULT": "对比图结果文案",
}


# ─── 使用日志 ─────────────────────────────────

def get_display_name(tg_user) -> str:
    name = tg_user.full_name or tg_user.first_name or str(tg_user.id)
    if tg_user.username:
        name += f" (@{tg_user.username})"
    return name


async def log_usage(tg_user, api_key: Optional[str], prompt: str, cost_time: str = ""):
    timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_id      = str(tg_user.id)
    display_name = get_display_name(tg_user)
    masked_key   = mask_api_key(api_key)
    row = [timestamp, user_id, display_name, masked_key, prompt, cost_time]
    async with _log_lock:
        write_header = not LOG_FILE.exists()
        with open(LOG_FILE, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["时间", "用户ID", "用户名", "API Key（脱敏）", "提示词", "耗时(s)"])
            writer.writerow(row)
    logger.info("使用日志：%s [%s] key=%s prompt=%s cost=%ss", display_name, user_id, masked_key, prompt[:30], cost_time)


# ─── 凌晨消息清理注册表 ───────────────────────

def _read_cleanup_registry_unlocked() -> dict:
    if CLEANUP_FILE.exists():
        try:
            return json.loads(CLEANUP_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _write_cleanup_registry_unlocked(registry: dict):
    USER_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CLEANUP_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(registry, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, CLEANUP_FILE)


async def _load_cleanup_registry() -> dict:
    async with _cleanup_lock:
        return await asyncio.to_thread(_read_cleanup_registry_unlocked)


async def _save_cleanup_registry(registry: dict):
    async with _cleanup_lock:
        await asyncio.to_thread(_write_cleanup_registry_unlocked, registry)


async def _pop_cleanup_registry() -> dict:
    async with _cleanup_lock:
        registry = await asyncio.to_thread(_read_cleanup_registry_unlocked)
        await asyncio.to_thread(_write_cleanup_registry_unlocked, {})
        return registry


async def register_for_cleanup(chat_id: int, *message_ids: int):
    """将一条或多条消息登记到凌晨清理名单。"""
    async with _cleanup_lock:
        registry = await asyncio.to_thread(_read_cleanup_registry_unlocked)
        key = str(chat_id)
        existing = set(registry.get(key, []))
        for mid in message_ids:
            existing.add(mid)
        registry[key] = list(existing)
        await asyncio.to_thread(_write_cleanup_registry_unlocked, registry)


async def midnight_cleanup_job(context):
    """JobQueue 回调：凌晨 0:00（北京时间）批量删除所有注册消息。"""
    registry = await _pop_cleanup_registry()
    deleted, skipped = 0, 0
    for chat_id_str, msg_ids in registry.items():
        chat_id = int(chat_id_str)
        for msg_id in msg_ids:
            # 撞 Telegram 限速时按 RetryAfter 等一下再试一次；其余异常视为已不存在跳过。
            for attempt in range(2):
                try:
                    await context.bot.delete_message(chat_id, msg_id)
                    deleted += 1
                    break
                except RetryAfter as ra:
                    if attempt == 0:
                        await asyncio.sleep(getattr(ra, "retry_after", 1) + 0.5)
                        continue
                    skipped += 1
                except Exception:
                    skipped += 1  # 消息已删或已不存在，跳过
                    break
            # 主动让出事件循环 + 轻微节流，避免一次性发太密把自己撞限速
            await asyncio.sleep(0.05)
    ds_histories.clear()
    # 注：pending_preset_saves / ds_pending 各自有自毁任务负责清，
    # 凌晨不再 clear，避免与正在飞行的等待流程产生 race。
    for token, info in list(pending_video_outfit_sources.items()):
        cleanup_pending_video_outfit_source(token, info)
    for token, info in list(pending_talking_video_audios.items()):
        cleanup_pending_talking_video_audio(token, info)
    # 清除所有等待用户补充输入的轻量状态（凌晨过期）
    for uid, st in list(user_states.items()):
        if st.get("state") in WAITING_STATE_LABELS:
            async with get_user_state_lock(uid):
                current = user_states.get(uid)
                if current and current.get("state") == st.get("state"):
                    user_states.pop(uid, None)
    # 清理无状态用户的锁，防止 user_state_locks 无限增长
    for uid in list(user_state_locks.keys()):
        if uid not in user_states:
            user_state_locks.pop(uid, None)
    logger.info("凌晨清理完成：删除 %d 条，跳过 %d 条，DS 历史已重置", deleted, skipped)
    cleanup_result_archive()


# ─── 数据持久化 ───────────────────────────────

def _read_data_unlocked() -> dict:
    if DATA_FILE.exists():
        try:
            data = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("users"), dict):
                return data
            logger.error("User_data.json 结构异常，将备份并重建。")
        except Exception as e:
            logger.error("User_data.json 解析失败：%s，将备份并重建。", e)
        # 损坏时备份原文件，避免被空数据覆盖导致用户数据无声丢失
        try:
            backup = DATA_FILE.with_suffix(
                f".broken.{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
            )
            os.replace(DATA_FILE, backup)
            logger.error("已将损坏的 User_data.json 备份为：%s", backup.name)
        except Exception as e:
            logger.error("备份损坏的 User_data.json 失败：%s", e)
    return {"users": {}}


def _write_data_unlocked(data: dict):
    USER_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = DATA_FILE.with_suffix(".tmp")
    try:
        tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp_path, DATA_FILE)
    except Exception:
        # 写入或替换失败时清理残留的 .tmp 文件，避免磁盘垃圾或下次读到半成品
        with contextlib.suppress(Exception):
            if tmp_path.exists():
                tmp_path.unlink()
        raise


def resolve_image_preset_path(
    stored_path: Optional[str],
    user_id: int,
    preset_name: Optional[str] = None,
) -> Optional[Path]:
    """把旧的预设路径解析到当前工作目录下的真实文件。

    兼容历史上保存过的绝对路径、搬家后的旧路径，以及只剩文件名的情况。
    """
    if not stored_path:
        return None

    user_dir = PRESET_IMAGE_DIR / str(user_id)
    raw_path = Path(stored_path)
    safe_name = re.sub(r"[^\w\u4e00-\u9fff\-]", "_", preset_name) if preset_name else None

    candidates = [raw_path]
    if raw_path.name:
        candidates.append(user_dir / raw_path.name)
    if safe_name:
        if raw_path.suffix:
            candidates.append(user_dir / f"{safe_name}{raw_path.suffix}")
        candidates.append(user_dir / safe_name)
        if user_dir.exists():
            try:
                matches = sorted(
                    user_dir.glob(f"{safe_name}.*"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                candidates.extend(matches)
            except Exception:
                pass

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        try:
            if candidate.is_file():
                return candidate
        except Exception:
            continue
    return None


def repair_image_preset_paths_unlocked(data: dict) -> bool:
    """把 image_presets 中可修复的旧路径写回当前路径。"""
    users = data.get("users")
    if not isinstance(users, dict):
        return False

    changed = False
    for uid_str, user in users.items():
        if not isinstance(user, dict):
            continue
        try:
            user_id = int(uid_str)
        except (TypeError, ValueError):
            continue
        presets = user.get("image_presets")
        if not isinstance(presets, dict) or not presets:
            continue

        for preset_name, stored_path in list(presets.items()):
            resolved = resolve_image_preset_path(stored_path, user_id, preset_name)
            if resolved and str(resolved) != stored_path:
                presets[preset_name] = str(resolved)
                changed = True

    return changed


async def load_data() -> dict:
    # 文件 IO 通过 to_thread 走线程池，避免阻塞事件循环。
    async with _data_lock:
        data = await asyncio.to_thread(_read_data_unlocked)
        if repair_image_preset_paths_unlocked(data):
            await asyncio.to_thread(_write_data_unlocked, data)
        return data


async def save_data(data: dict):
    async with _data_lock:
        await asyncio.to_thread(_write_data_unlocked, data)


async def update_data(mutator):
    """在同一把锁里完成读-改-写，避免并发更新覆盖彼此。"""
    async with _data_lock:
        data = await asyncio.to_thread(_read_data_unlocked)
        repair_image_preset_paths_unlocked(data)
        result = mutator(data)
        await asyncio.to_thread(_write_data_unlocked, data)
        return result


def get_user(data: dict, user_id: int) -> dict:
    uid = str(user_id)
    if uid not in data["users"]:
        data["users"][uid] = {"api_key": None, "prompts": {}, "compare_switches": {}, "compare_origin_text": "", "compare_result_text": ""}
    user = data["users"][uid]
    user.setdefault("prompts", {})
    user.setdefault("image_presets", {})
    user.setdefault("voice_presets", {})
    user.setdefault("compare_switches", {})
    user.setdefault("compare_origin_text", "")
    user.setdefault("compare_result_text", "")
    normalize_image_extend_switches(user)
    normalize_animation_seconds(user)
    return user


def get_user_first_last_prompt(user: dict) -> str:
    prompt = (user.get("first_last_prompt") or "").strip()
    return prompt or DEFAULT_FIRST_LAST_VIDEO_PROMPT


def get_user_talking_video_prompt(user: dict) -> str:
    prompt = (user.get("talking_video_prompt") or "").strip()
    return prompt or DEFAULT_TALKING_VIDEO_PROMPT


def normalize_animation_seconds(user: dict) -> int:
    try:
        seconds = int(user.get("animation_seconds", DEFAULT_ANIMATION_SECONDS))
    except (TypeError, ValueError):
        seconds = DEFAULT_ANIMATION_SECONDS
    if seconds not in ANIMATION_SECONDS_OPTIONS:
        seconds = DEFAULT_ANIMATION_SECONDS
    user["animation_seconds"] = seconds
    return seconds


def normalize_image_extend_switches(user: dict) -> dict[str, bool]:
    switches = user.setdefault("image_extend_switches", {})
    if not isinstance(switches, dict):
        switches = {}
        user["image_extend_switches"] = switches
    for direction in IMAGE_EXTEND_DIRECTIONS:
        switches.setdefault(direction, True)
        switches[direction] = bool(switches[direction])
    return switches


def build_image_extend_values(user: dict) -> dict[str, int]:
    switches = normalize_image_extend_switches(user)
    return {
        direction: IMAGE_EXTEND_PIXELS if switches.get(direction, True) else 0
        for direction in IMAGE_EXTEND_DIRECTIONS
    }


def build_image_extend_keyboard(user: dict) -> InlineKeyboardMarkup:
    values = build_image_extend_values(user)

    def _button(direction: str) -> InlineKeyboardButton:
        label = IMAGE_EXTEND_LABELS[direction]
        value = values[direction]
        status = "✅" if value else "❌"
        return InlineKeyboardButton(f"{status} {label} {value}px", callback_data=f"expand:{direction}")

    return InlineKeyboardMarkup([
        [_button("top"), _button("bottom")],
        [_button("left"), _button("right")],
    ])


def mask_api_key(api_key: Optional[str]) -> str:
    if not api_key:
        return "未设置"
    if len(api_key) <= 8:
        return api_key[:2] + "***"
    return f"{api_key[:4]}***{api_key[-4:]}"


def log_update_received(update: Update, route: str):
    user = update.effective_user
    chat = update.effective_chat
    msg = update.effective_message
    logger.info(
        "收到 Telegram 更新：route=%s update_id=%s user=%s chat=%s msg=%s",
        route,
        update.update_id,
        getattr(user, "id", None),
        getattr(chat, "id", None),
        getattr(msg, "message_id", None),
    )


def schedule_background_task(coro):
    task = asyncio.create_task(coro)
    active_tasks.add(task)

    def _cleanup(done_task: asyncio.Task):
        active_tasks.discard(done_task)
        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("后台任务执行失败")

    task.add_done_callback(_cleanup)
    return task


def build_user_task_limit_message(count: Optional[int] = None) -> str:
    count_text = count if count is not None else MAX_ACTIVE_TASKS_PER_USER
    return f"当前已有 {count_text} 个任务在处理中，请等完成后再点。"


async def get_user_active_task_count(user_id: int) -> int:
    async with _user_task_count_lock:
        return user_active_task_counts.get(user_id, 0)


async def is_user_task_limit_reached(user_id: int) -> bool:
    return await get_user_active_task_count(user_id) >= MAX_ACTIVE_TASKS_PER_USER


async def release_user_task_slot(user_id: int):
    async with _user_task_count_lock:
        count = user_active_task_counts.get(user_id, 0) - 1
        if count > 0:
            user_active_task_counts[user_id] = count
        else:
            user_active_task_counts.pop(user_id, None)


async def schedule_user_processing_task(user_id: int, coro_factory) -> tuple[bool, int]:
    async with _user_task_count_lock:
        count = user_active_task_counts.get(user_id, 0)
        if count >= MAX_ACTIVE_TASKS_PER_USER:
            return False, count
        user_active_task_counts[user_id] = count + 1
        new_count = count + 1

    async def _runner():
        try:
            await coro_factory()
        finally:
            await release_user_task_slot(user_id)

    schedule_background_task(_runner())
    return True, new_count


async def open_shared_http_session() -> aiohttp.ClientSession:
    global shared_http_session
    async with _http_lock:
        if shared_http_session is None or shared_http_session.closed:
            shared_http_session = aiohttp.ClientSession(timeout=SHARED_HTTP_TIMEOUT)
            logger.info("共享 HTTP Session 已创建")
        return shared_http_session


async def get_shared_http_session() -> aiohttp.ClientSession:
    """获取共享 HTTP Session，若不存在或已关闭则自动重建，不再抛错。"""
    global shared_http_session
    async with _http_lock:
        if shared_http_session is None or shared_http_session.closed:
            shared_http_session = aiohttp.ClientSession(timeout=SHARED_HTTP_TIMEOUT)
            logger.warning("共享 HTTP Session 不存在或已关闭，已自动重建")
        return shared_http_session


async def close_shared_http_session():
    global shared_http_session
    async with _http_lock:
        session = shared_http_session
        shared_http_session = None
    if session is not None and not session.closed:
        await session.close()
        logger.info("共享 HTTP Session 已关闭")


async def cancel_active_tasks():
    tasks = [task for task in list(active_tasks) if not task.done()]
    if not tasks:
        return

    logger.info("正在取消 %d 个后台任务", len(tasks))
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def get_user_state_lock(user_id: int) -> asyncio.Lock:
    lock = user_state_locks.get(user_id)
    if lock is None:
        lock = asyncio.Lock()
        user_state_locks[user_id] = lock
    return lock


async def claim_user_state(
    user_id: int,
    desired_state: dict,
    state_name: str,
    session_id: Optional[str] = None,
    save_id: Optional[str] = None,
) -> tuple[bool, Optional[dict], bool]:
    """原子地检查并设置用户等待状态。

    返回 (success, current_state, already_same)：
    - success=False: 当前已有别的等待流程占用该用户
    - already_same=True: 目标状态已经存在，视为幂等重复点击
    """
    async with get_user_state_lock(user_id):
        current_state = user_states.get(user_id)
        if current_state and current_state.get("state") in WAITING_STATE_LABELS:
            if is_same_waiting_state(
                current_state,
                state_name,
                session_id=session_id,
                save_id=save_id,
            ):
                return True, current_state, True
            return False, current_state, False

        user_states[user_id] = desired_state
        return True, current_state, False


async def pop_user_state_if_same(
    user_id: int,
    state_name: str,
    session_id: Optional[str] = None,
    save_id: Optional[str] = None,
) -> Optional[dict]:
    async with get_user_state_lock(user_id):
        current_state = user_states.get(user_id)
        if is_same_waiting_state(
            current_state,
            state_name,
            session_id=session_id,
            save_id=save_id,
        ):
            return user_states.pop(user_id, None)
        return None


async def reply_text_with_fallback(msg, text: str, **kwargs):
    try:
        return await msg.reply_text(text, **kwargs)
    except Exception:
        return await msg.get_bot().send_message(chat_id=msg.chat_id, text=text, **kwargs)


async def safe_edit_text(message, text: str, *, log_context: str = "状态消息", **kwargs) -> bool:
    """编辑 Telegram 消息；被限流或消息不可编辑时只记录，不中断主流程。"""
    chat_id = message.chat_id
    now = time.time()
    blocked = _status_edit_blocked_until.get(chat_id, 0)
    if now < blocked:
        logger.debug(
            "跳过%s编辑（Telegram 限流冷却中，剩余 %.0fs）：%s",
            log_context,
            blocked - now,
            text,
        )
        return False
    elif blocked:
        _status_edit_blocked_until.pop(chat_id, None)

    try:
        await message.edit_text(text, **kwargs)
        return True
    except RetryAfter as e:
        retry_after = int(getattr(e, "retry_after", 0) or 0)
        _status_edit_blocked_until[chat_id] = now + retry_after
        logger.warning(
            "Telegram %s编辑触发限流，%ss 内跳过状态更新：%s",
            log_context,
            retry_after,
            text,
        )
        return False
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        logger.warning("Telegram %s编辑失败：%s", log_context, e)
        return False


async def safe_delete_message(message, *, log_context: str = "状态消息") -> bool:
    try:
        await message.delete()
        return True
    except RetryAfter as e:
        logger.warning(
            "Telegram %s删除触发限流，跳过删除（retry_after=%ss）",
            log_context,
            int(getattr(e, "retry_after", 0) or 0),
        )
        return False
    except Exception as e:
        logger.debug("Telegram %s删除失败：%s", log_context, e)
        return False


async def reply_autodelete(target, text: str, seconds: int = WARNING_DELETE_SECONDS, also_delete=None, **kwargs):
    """发送一条即时反馈消息（成功/失败/警告），N 秒后自动删除。"""
    sent = await target.reply_text(text, **kwargs)
    schedule_background_task(
        delete_message_later(sent.get_bot(), sent.chat_id, sent.message_id, seconds)
    )
    if also_delete is not None:
        schedule_background_task(
            delete_message_later(also_delete.get_bot(), also_delete.chat_id, also_delete.message_id, seconds)
    )
    return sent


async def send_autodelete_message(bot, chat_id: int, text: str, seconds: int = WARNING_DELETE_SECONDS, **kwargs):
    sent = await bot.send_message(chat_id=chat_id, text=text, **kwargs)
    schedule_background_task(delete_message_later(bot, chat_id, sent.message_id, seconds))
    return sent


async def reply_document_with_fallback(msg, document: bytes, filename: str, caption: str, **kwargs):
    """发送文件，自动放宽超时、失败时最多重试一次。"""
    # 图片/视频文件可能较大，显式放宽写入超时（PTB 默认 20s 往往不够）
    send_kwargs = {
        "write_timeout": MEDIA_TRANSFER_TIMEOUT,
        "read_timeout": MEDIA_TRANSFER_TIMEOUT,
        **kwargs,
    }
    for attempt in range(2):
        try:
            try:
                return await msg.reply_document(
                    document=document, filename=filename, caption=caption, **send_kwargs
                )
            except Exception:
                return await msg.get_bot().send_document(
                    chat_id=msg.chat_id,
                    document=document,
                    filename=filename,
                    caption=caption,
                    **send_kwargs,
                )
        except Exception:
            if attempt == 0:
                logger.warning("发送文件失败，1 秒后重试……")
                await asyncio.sleep(1)
            else:
                raise


async def reply_video_with_fallback(msg, video: bytes, filename: str, caption: str, **kwargs):
    """发送视频预览，失败时回退到文档发送。"""
    doc_kwargs = dict(kwargs)
    doc_kwargs.pop("supports_streaming", None)
    send_kwargs = {
        "write_timeout": MEDIA_TRANSFER_TIMEOUT,
        "read_timeout": MEDIA_TRANSFER_TIMEOUT,
        "supports_streaming": True,
        **kwargs,
    }
    for attempt in range(2):
        try:
            try:
                return await msg.reply_video(
                    video=video,
                    filename=filename,
                    caption=caption,
                    **send_kwargs,
                )
            except Exception:
                return await msg.get_bot().send_video(
                    chat_id=msg.chat_id,
                    video=video,
                    filename=filename,
                    caption=caption,
                    **send_kwargs,
                )
        except Exception:
            if attempt == 0:
                logger.warning("发送视频预览失败，1 秒后重试……")
                await asyncio.sleep(1)
            else:
                logger.warning("发送视频预览失败，回退为文档发送……")
                return await reply_document_with_fallback(
                    msg,
                    document=video,
                    filename=filename,
                    caption=caption,
                    **doc_kwargs,
                )


async def reply_media_with_document_for_images(
    msg,
    media: bytes,
    filename: str,
    caption: str,
    extension: Optional[str],
    **kwargs,
):
    """图片结果始终按文件发送；视频结果保留预览发送。"""
    if is_document_image_extension(extension):
        return await reply_document_with_fallback(
            msg,
            document=media,
            filename=filename,
            caption=caption,
            **kwargs,
        )
    return await reply_video_with_fallback(
        msg,
        video=media,
        filename=filename,
        caption=caption,
        **kwargs,
    )


async def reply_audio_with_fallback(msg, audio: bytes, filename: str, caption: str, **kwargs):
    """发送音频，失败时回退为文件发送，确保用户能下载。"""
    send_kwargs = {
        "write_timeout": MEDIA_TRANSFER_TIMEOUT,
        "read_timeout": MEDIA_TRANSFER_TIMEOUT,
        **kwargs,
    }
    for attempt in range(2):
        try:
            try:
                return await msg.reply_audio(
                    audio=audio,
                    filename=filename,
                    caption=caption,
                    **send_kwargs,
                )
            except Exception:
                return await msg.get_bot().send_audio(
                    chat_id=msg.chat_id,
                    audio=audio,
                    filename=filename,
                    caption=caption,
                    **send_kwargs,
                )
        except Exception:
            if attempt == 0:
                logger.warning("发送音频失败，1 秒后重试……")
                await asyncio.sleep(1)
            else:
                logger.warning("发送音频失败，回退为文档发送……")
                return await reply_document_with_fallback(
                    msg,
                    document=audio,
                    filename=filename,
                    caption=caption,
                    **kwargs,
                )


def prompt_contains_banned_terms(prompt: str) -> Optional[str]:
    text = prompt.lower()

    for term, message in BANNED_PROMPT_MESSAGES.items():
        if term == "loli":
            if "loli" in text:
                return message
            continue

        if term in prompt:
            return message

    if re.search(r"(?i)(?<![a-z])child(?![a-z])", prompt):
        return "提示词包含禁用词：child"

    return None


def validate_prompt_text(prompt: str) -> Optional[str]:
    prompt = prompt.strip()
    if not prompt:
        return "提示词不能为空"
    return prompt_contains_banned_terms(prompt)


def build_prompt_preview(prompt: str, limit: int = 5) -> str:
    text = prompt.strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def build_upload_status_message() -> str:
    return "☁️ 上传中…"


def build_processing_start_message(prompt: str) -> str:
    return f"⚙️ 处理中：{build_prompt_preview(prompt)}"


def build_result_filename(index: int, total: int) -> str:
    if total > 1:
        return f"Image_{index + 1}_of_{total}.png"
    return "Image.png"


def build_animation_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{extension.lower().lstrip('.')}" if extension else Path(url.split("?", 1)[0]).suffix.lower()
    if suffix not in (".mp4", ".gif", ".webm", ".mov", ".png", ".jpg", ".jpeg", ".webp"):
        suffix = ".mp4"
    if total > 1:
        return f"Animation_{index + 1}_of_{total}{suffix}"
    return f"Animation{suffix}"


def build_first_last_video_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{extension.lower().lstrip('.')}" if extension else Path(url.split("?", 1)[0]).suffix.lower()
    if suffix not in (".mp4", ".gif", ".webm", ".mov"):
        suffix = ".mp4"
    if total > 1:
        return f"FirstLastVideo_{index + 1}_of_{total}{suffix}"
    return f"FirstLastVideo{suffix}"


def build_voice_result_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{extension.lower().lstrip('.')}" if extension else Path(url.split("?", 1)[0]).suffix.lower()
    if suffix not in VOICE_RESULT_SUFFIXES:
        suffix = ".mp3"
    if total > 1:
        return f"Voice_{index + 1}_of_{total}{suffix}"
    return f"Voice{suffix}"


def build_talking_video_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{normalize_video_outfit_extension(url, extension)}"
    if total > 1:
        return f"TalkingVideo_{index + 1}_of_{total}{suffix}"
    return f"TalkingVideo{suffix}"


def compute_image_longest_side(image_bytes: bytes) -> Optional[int]:
    """读取图片字节，返回最长边像素；失败返回 None。"""
    try:
        from PIL import Image
        import io
        with Image.open(io.BytesIO(image_bytes)) as img:
            return max(img.width, img.height)
    except Exception:
        logger.exception("读取图片尺寸失败")
        return None


VIDEO_OUTFIT_MEDIA_SUFFIXES = {".mp4", ".gif", ".webm", ".mov", ".png", ".jpg", ".jpeg", ".webp"}


def normalize_video_outfit_extension(url: str, extension: Optional[str] = None) -> str:
    suffix = f".{extension.lower().lstrip('.')}" if extension else Path(url.split("?", 1)[0]).suffix.lower()
    if suffix not in VIDEO_OUTFIT_MEDIA_SUFFIXES:
        suffix = ".mp4"
    return suffix.lstrip(".")


def is_document_image_extension(extension: Optional[str]) -> bool:
    if not extension:
        return False
    return f".{extension.lower().lstrip('.')}" in DOCUMENT_IMAGE_SUFFIXES


def build_video_outfit_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{normalize_video_outfit_extension(url, extension)}"
    if total > 1:
        return f"VideoOutfit_{index + 1}_of_{total}{suffix}"
    return f"VideoOutfit{suffix}"


def build_video_outfit_source_filename(url: str, index: int, total: int, extension: Optional[str] = None) -> str:
    suffix = f".{normalize_video_outfit_extension(url, extension)}"
    if total > 1:
        return f"VideoOutfitSource_{index + 1}_of_{total}{suffix}"
    return f"VideoOutfitSource{suffix}"


def build_video_outfit_source_path(token: str, extension: str) -> Path:
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    suffix = extension.lower().lstrip(".") or "mp4"
    if f".{suffix}" not in VIDEO_OUTFIT_MEDIA_SUFFIXES:
        suffix = "mp4"
    return PENDING_PRESET_DIR / f"video_outfit_source_{token}.{suffix}"


def build_video_outfit_source_meta_path(token: str) -> Path:
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    return PENDING_PRESET_DIR / f"video_outfit_source_{token}.json"


def write_video_outfit_source_meta(token: str, source_info: dict):
    meta_path = build_video_outfit_source_meta_path(token)
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(source_info, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, meta_path)


def read_video_outfit_source_meta(token: str) -> Optional[dict]:
    meta_path = build_video_outfit_source_meta_path(token)
    if not meta_path.exists():
        return None
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("视频源文件元数据读取失败：%s", meta_path)
        return None
    return data if isinstance(data, dict) else None


def register_pending_video_outfit_source(
    token: str,
    user_id: int,
    chat_id: int,
    path: str,
    filename: str,
) -> dict:
    source_info = {
        "user_id": user_id,
        "chat_id": chat_id,
        "path": path,
        "filename": filename,
    }
    pending_video_outfit_sources[token] = source_info
    write_video_outfit_source_meta(token, source_info)
    return source_info


def build_talking_video_audio_path(token: str, extension: str) -> Path:
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    suffix = extension.lower().lstrip(".") or "mp3"
    if f".{suffix}" not in VOICE_RESULT_SUFFIXES:
        suffix = "mp3"
    return PENDING_PRESET_DIR / f"talking_video_audio_{token}.{suffix}"


def build_talking_video_audio_meta_path(token: str) -> Path:
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    return PENDING_PRESET_DIR / f"talking_video_audio_{token}.json"


def write_talking_video_audio_meta(token: str, audio_info: dict):
    meta_path = build_talking_video_audio_meta_path(token)
    tmp_path = meta_path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(audio_info, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp_path, meta_path)


def read_talking_video_audio_meta(token: str) -> Optional[dict]:
    meta_path = build_talking_video_audio_meta_path(token)
    if not meta_path.exists():
        return None
    try:
        data = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("说话视频音频元数据读取失败：%s", meta_path)
        return None
    return data if isinstance(data, dict) else None


def register_pending_talking_video_audio(
    token: str,
    user_id: int,
    chat_id: int,
    path: str,
    filename: str,
    content_type: str,
    duration_seconds: int,
) -> dict:
    audio_info = {
        "user_id": user_id,
        "chat_id": chat_id,
        "path": path,
        "filename": filename,
        "content_type": content_type,
        "duration_seconds": duration_seconds,
    }
    pending_talking_video_audios[token] = audio_info
    write_talking_video_audio_meta(token, audio_info)
    return audio_info


def build_prompt_validation_reply(error: str) -> str:
    return (
        f"❌ 不能处理这张图：{error}\n"
        "请重新输入一个不含禁用词的提示词。"
    )


def is_selection_rate_limited(session: dict, now: Optional[float] = None) -> bool:
    if now is None:
        now = time.time()
    return now < float(session.get("selection_lock_until", 0))


def mark_selection_clicked(session: dict, now: Optional[float] = None):
    if now is None:
        now = time.time()
    session["selection_lock_until"] = now + 1


def dedupe_preserving_order(items: list[str]) -> list[str]:
    seen = set()
    unique_items = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        unique_items.append(item)
    return unique_items


def is_same_waiting_state(
    state: Optional[dict],
    state_name: str,
    session_id: Optional[str] = None,
    save_id: Optional[str] = None,
) -> bool:
    if not state or state.get("state") != state_name:
        return False
    if session_id is not None and state.get("session_id") != session_id:
        return False
    if save_id is not None and state.get("save_id") != save_id:
        return False
    return True


def get_waiting_state_conflict(
    user_id: int,
    state_name: str,
    session_id: Optional[str] = None,
    save_id: Optional[str] = None,
) -> Optional[dict]:
    current_state = user_states.get(user_id)
    if not current_state:
        return None
    if is_same_waiting_state(current_state, state_name, session_id=session_id, save_id=save_id):
        return None
    if current_state.get("state") in WAITING_STATE_LABELS:
        return current_state
    return None


def build_waiting_state_conflict_message(state: dict) -> str:
    label = WAITING_STATE_LABELS.get(state.get("state"), "当前")
    return f"请先完成「{label}」流程，或重新发送图片开始新的操作。"


def normalize_image_content_type(content_type: Optional[str], filename: Optional[str] = None) -> str:
    if content_type and content_type.startswith("image/"):
        return content_type
    guessed_type, _encoding = mimetypes.guess_type(filename or "")
    if guessed_type and guessed_type.startswith("image/"):
        return guessed_type
    return DEFAULT_IMAGE_CONTENT_TYPE


def image_filename_for_upload(filename: Optional[str], default_name: str, content_type: Optional[str] = None) -> str:
    name = Path(filename or default_name).name or default_name
    if Path(name).suffix:
        return name
    normalized_type = normalize_image_content_type(content_type, name)
    return name + IMAGE_MIME_EXTENSIONS.get(normalized_type, ".jpg")


def parse_saveimg_name(text: Optional[str]) -> Optional[str]:
    parts = (text or "").strip().split(maxsplit=1)
    if not parts:
        return None
    command = parts[0].split("@", 1)[0].lower()
    if command != "/saveimg":
        return None
    return parts[1].strip() if len(parts) > 1 else ""


def normalize_video_content_type(content_type: Optional[str], filename: Optional[str] = None) -> str:
    if content_type and content_type.startswith("video/"):
        return content_type
    guessed_type, _encoding = mimetypes.guess_type(filename or "")
    if guessed_type and guessed_type.startswith("video/"):
        return guessed_type
    return DEFAULT_VIDEO_CONTENT_TYPE


def video_filename_for_upload(filename: Optional[str], default_name: str, content_type: Optional[str] = None) -> str:
    name = Path(filename or default_name).name or default_name
    if Path(name).suffix:
        return name
    normalized_type = normalize_video_content_type(content_type, name)
    return name + VIDEO_MIME_EXTENSIONS.get(normalized_type, ".mp4")


def image_extension_for_save(content_type: Optional[str], filename: Optional[str] = None) -> str:
    normalized_type = normalize_image_content_type(content_type, filename)
    return IMAGE_MIME_EXTENSIONS.get(normalized_type, Path(filename or "").suffix or ".jpg")


def video_extension_for_save(content_type: Optional[str], filename: Optional[str] = None) -> str:
    normalized_type = normalize_video_content_type(content_type, filename)
    return VIDEO_MIME_EXTENSIONS.get(normalized_type, Path(filename or "").suffix or ".mp4")


def normalize_audio_content_type(content_type: Optional[str], filename: Optional[str] = None) -> str:
    if content_type and (
        content_type.startswith("audio/")
        or content_type in {"application/ogg", "video/ogg", "video/webm"}
    ):
        return content_type
    guessed_type, _encoding = mimetypes.guess_type(filename or "")
    if guessed_type and (
        guessed_type.startswith("audio/")
        or guessed_type in {"application/ogg", "video/ogg", "video/webm"}
    ):
        return guessed_type
    return DEFAULT_AUDIO_CONTENT_TYPE


def audio_filename_for_upload(filename: Optional[str], default_name: str, content_type: Optional[str] = None) -> str:
    name = Path(filename or default_name).name or default_name
    if Path(name).suffix:
        return name
    normalized_type = normalize_audio_content_type(content_type, name)
    return name + AUDIO_MIME_EXTENSIONS.get(normalized_type, ".mp3")


def audio_extension_for_save(content_type: Optional[str], filename: Optional[str] = None) -> str:
    normalized_type = normalize_audio_content_type(content_type, filename)
    suffix = AUDIO_MIME_EXTENSIONS.get(normalized_type)
    if suffix:
        return suffix
    fallback_suffix = Path(filename or "").suffix.lower()
    return fallback_suffix if fallback_suffix in VOICE_RESULT_SUFFIXES else ".mp3"


def audio_content_type_from_path(path: str) -> str:
    return normalize_audio_content_type(None, path)


def audio_content_type_from_bytes(audio_bytes: bytes, fallback: str = DEFAULT_AUDIO_CONTENT_TYPE) -> str:
    if audio_bytes.startswith(b"ID3") or audio_bytes[:2] == b"\xff\xfb":
        return "audio/mpeg"
    if audio_bytes.startswith(b"RIFF") and audio_bytes[8:12] == b"WAVE":
        return "audio/wav"
    if audio_bytes.startswith(b"OggS"):
        return "audio/ogg"
    if audio_bytes.startswith(b"fLaC"):
        return "audio/flac"
    if audio_bytes.startswith(b"\x1a\x45\xdf\xa3"):
        return "audio/webm"
    return fallback


def resolve_ffmpeg_path() -> Optional[str]:
    for candidate in (
        shutil.which("ffmpeg"),
        "/opt/homebrew/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "/usr/bin/ffmpeg",
    ):
        if candidate and Path(candidate).exists():
            return candidate
    return None


def resolve_ffprobe_path() -> Optional[str]:
    ffmpeg_path = resolve_ffmpeg_path()
    ffmpeg_dir = str(Path(ffmpeg_path).parent / "ffprobe") if ffmpeg_path else None
    for candidate in (
        shutil.which("ffprobe"),
        ffmpeg_dir,
        "/opt/homebrew/bin/ffprobe",
        "/usr/local/bin/ffprobe",
        "/usr/bin/ffprobe",
    ):
        if candidate and Path(candidate).exists():
            return candidate
    return None


def media_extension_for_probe(content_type: Optional[str], filename: Optional[str]) -> str:
    if content_type and content_type.startswith("video/"):
        return video_extension_for_save(content_type, filename)
    if content_type and (
        content_type.startswith("audio/")
        or content_type in {"application/ogg", "video/ogg", "video/webm"}
    ):
        return audio_extension_for_save(content_type, filename)
    suffix = Path(filename or "").suffix
    return suffix or ".bin"


def normalize_talking_video_seconds(duration_seconds: Optional[float]) -> int:
    try:
        seconds = math.ceil(float(duration_seconds or 0))
    except (TypeError, ValueError):
        seconds = 0
    return max(seconds, 1) if seconds > 0 else TALKING_VIDEO_DEFAULT_SECONDS


async def probe_media_duration_seconds(
    media_bytes: bytes,
    filename: str,
    content_type: str,
) -> Optional[float]:
    ffprobe_path = resolve_ffprobe_path()
    if not ffprobe_path:
        return None

    token = uuid.uuid4().hex[:12]
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    input_path = PENDING_PRESET_DIR / f"duration_probe_{token}{media_extension_for_probe(content_type, filename)}"
    try:
        await asyncio.to_thread(input_path.write_bytes, media_bytes)
        proc = await asyncio.create_subprocess_exec(
            ffprobe_path,
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(input_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            with contextlib.suppress(Exception):
                proc.kill()
            return None
        if proc.returncode != 0:
            detail = (stderr or b"").decode("utf-8", errors="ignore")
            logger.warning("ffprobe 获取媒体时长失败：%s", detail[-1000:])
            return None
        value = (stdout or b"").decode("utf-8", errors="ignore").strip()
        return float(value) if value else None
    except Exception:
        logger.exception("探测媒体时长失败：filename=%s", filename)
        return None
    finally:
        cleanup_temp_file(str(input_path))


async def get_talking_video_duration_seconds(
    media_bytes: bytes,
    filename: str,
    content_type: str,
    hinted_duration: Optional[float] = None,
) -> int:
    if hinted_duration:
        return normalize_talking_video_seconds(hinted_duration)
    probed = await probe_media_duration_seconds(media_bytes, filename, content_type)
    return normalize_talking_video_seconds(probed)


async def extract_audio_from_video_bytes(
    video_bytes: bytes,
    video_filename: str,
    video_content_type: str,
) -> tuple[bytes, str, str]:
    ffmpeg_path = resolve_ffmpeg_path()
    if not ffmpeg_path:
        raise RuntimeError("当前电脑没有找到 ffmpeg，暂时不能从视频提取音频。")

    token = uuid.uuid4().hex[:12]
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    input_path = PENDING_PRESET_DIR / f"voice_sample_video_{token}{video_extension_for_save(video_content_type, video_filename)}"
    output_path = PENDING_PRESET_DIR / f"voice_sample_audio_{token}.m4a"
    try:
        await asyncio.to_thread(input_path.write_bytes, video_bytes)
        proc = await asyncio.create_subprocess_exec(
            ffmpeg_path,
            "-y",
            "-i",
            str(input_path),
            "-map",
            "0:a:0",
            "-vn",
            "-acodec",
            "aac",
            "-b:a",
            "128k",
            str(output_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=FFMPEG_AUDIO_EXTRACT_TIMEOUT,
            )
        except asyncio.TimeoutError as exc:
            with contextlib.suppress(Exception):
                proc.kill()
            raise RuntimeError("视频音轨提取超时，请换一段短一点的视频。") from exc

        if proc.returncode != 0:
            detail = (stderr or b"").decode("utf-8", errors="ignore")
            logger.warning("ffmpeg 提取音频失败：%s", detail[-1000:])
            raise RuntimeError("视频里没有可用音轨，或音轨提取失败。")
        if not output_path.exists() or output_path.stat().st_size <= 0:
            raise RuntimeError("视频音轨提取失败，输出为空。")
        audio_bytes = await asyncio.to_thread(output_path.read_bytes)
        return audio_bytes, "voice_sample.m4a", "audio/mp4"
    finally:
        cleanup_temp_file(str(input_path))
        cleanup_temp_file(str(output_path))


def validate_voice_text(text: str) -> Optional[str]:
    text = text.strip()
    if not text:
        return "文案不能为空"
    if len(text) > VOICE_TEXT_MAX_CHARS:
        return f"文案太长了，最多 {VOICE_TEXT_MAX_CHARS} 字"
    return None


def image_content_type_from_path(path: str) -> str:
    return normalize_image_content_type(None, path)


def image_content_type_from_bytes(image_bytes: bytes, fallback: str = DEFAULT_IMAGE_CONTENT_TYPE) -> str:
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    if image_bytes.startswith(b"GIF87a") or image_bytes.startswith(b"GIF89a"):
        return "image/gif"
    return fallback


async def expire_prompt_session(session_id: str) -> Optional[dict]:
    # 先偷看一下 session 以拿到 user_id；真正的 pop 放到锁里做，
    # 避免 pop 与 user_state 检查之间出现竞态（另一协程可能在缝隙中修改状态）。
    peek = pending_prompt_sessions.get(session_id)
    if not peek:
        return None
    user_id = peek.get("user_id")

    async with get_user_state_lock(user_id):
        session = pending_prompt_sessions.pop(session_id, None)
        if not session:
            return None
        state = user_states.get(user_id)
        if state and state.get("session_id") == session_id and state.get("state") in WAITING_STATE_LABELS:
            user_states.pop(user_id, None)
    return session


async def expire_prompt_session_later(session_id: str, delay_seconds: Optional[int] = None):
    # 默认凌晨过期：session 生命周期和面板/原图保持一致，按钮全天可用。
    await asyncio.sleep(delay_seconds if delay_seconds is not None else seconds_until_midnight())
    session = await expire_prompt_session(session_id)
    if not session:
        return

    cleanup_temp_file(session.get("image_path"))

    bot = session.get("bot")
    chat_id = session.get("chat_id")
    if bot is not None and chat_id is not None:
        # 流程中 bot 发出的引导消息（"请发送脸图/参考图/提示词"），以及用户在这些流程中
        # 已经发送的半截输入（partial_input_msg_ids），过期时一并删除。
        # 注意：source_message_id（用户上传的原图）不在这里删，它属于🟢"原图+按钮面板"那档，
        # 由 midnight_cleanup_job 凌晨统一清理。
        cleanup_ids = []
        for key in (
            "face_request_msg_id",
            "body_request_msg_id",
            "qwen_request_msg_id",
            "scene_person_request_msg_id",
            "prompt_request_msg_id",
            "animation_prompt_request_msg_id",
            "last_frame_request_msg_id",
            "first_last_prompt_request_msg_id",
            "talking_video_audio_request_msg_id",
            "talking_video_image_request_msg_id",
        ):
            mid = session.get(key)
            if mid is not None:
                cleanup_ids.append(mid)
        cleanup_ids.extend(session.get("partial_input_msg_ids", []) or [])

        for mid in cleanup_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                # 消息可能已被成功路径删除或用户手动删除，安静跳过
                pass
    logger.info("提示词会话已自动过期并清理：session_id=%s", session_id)


async def expire_video_outfit_session(session_id: str) -> Optional[dict]:
    peek = pending_video_outfit_sessions.get(session_id)
    if not peek:
        return None
    user_id = peek.get("user_id")

    async with get_user_state_lock(user_id):
        session = pending_video_outfit_sessions.pop(session_id, None)
        if not session:
            return None
        state = user_states.get(user_id)
        if state and state.get("session_id") == session_id and state.get("state") in WAITING_STATE_LABELS:
            user_states.pop(user_id, None)
    return session


def cleanup_video_outfit_session(session_id: Optional[str], fallback_session: Optional[dict] = None) -> Optional[dict]:
    session = pending_video_outfit_sessions.pop(session_id, None) if session_id else None
    session = session or fallback_session
    if session:
        cleanup_temp_file(session.get("video_path"))
    return session


def cleanup_pending_video_outfit_source(token: Optional[str], fallback_source: Optional[dict] = None) -> Optional[dict]:
    source = pending_video_outfit_sources.pop(token, None) if token else None
    source = source or fallback_source
    if source:
        cleanup_temp_file(source.get("path"))
    if token:
        cleanup_temp_file(str(build_video_outfit_source_meta_path(token)))
    return source


def cleanup_pending_talking_video_audio(token: Optional[str], fallback_audio: Optional[dict] = None) -> Optional[dict]:
    audio_info = pending_talking_video_audios.pop(token, None) if token else None
    audio_info = audio_info or fallback_audio
    if audio_info:
        cleanup_temp_file(audio_info.get("path"))
    if token:
        cleanup_temp_file(str(build_talking_video_audio_meta_path(token)))
    return audio_info


async def expire_video_outfit_session_later(session_id: str, delay_seconds: Optional[int] = None):
    await asyncio.sleep(delay_seconds if delay_seconds is not None else seconds_until_midnight())
    session = await expire_video_outfit_session(session_id)
    if not session:
        return

    cleanup_temp_file(session.get("video_path"))
    bot = session.get("bot")
    chat_id = session.get("chat_id")
    request_msg_id = session.get("video_ref_request_msg_id")
    if bot is not None and chat_id is not None and request_msg_id is not None:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=chat_id, message_id=request_msg_id)
    logger.info("视频换衣会话已自动过期并清理：session_id=%s", session_id)


async def expire_waiting_flow_later(
    user_id: int,
    state_name: str,
    state_key: str,
    state_value: str,
    bot,
    chat_id: int,
    *message_ids: int,
    flow_id: Optional[str] = None,
    delay_seconds: int = WAITING_FLOW_TIMEOUT_SECONDS,
):
    """等待用户继续输入的流程消息，超时后清掉状态并删除引导消息。"""
    await asyncio.sleep(delay_seconds)
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if (
            state
            and state.get("state") == state_name
            and state.get(state_key) == state_value
            and (flow_id is None or state.get("flow_id") == flow_id)
        ):
            user_states.pop(user_id, None)

    for mid in message_ids:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, mid)


async def _expire_ds_confirm_later(ds_sid: str, expire_token: str, bot, chat_id: int, msg_id: int):
    """5分钟无操作：删除 DeepSeek 确认消息，清理 ds_pending。"""
    await asyncio.sleep(WAITING_FLOW_TIMEOUT_SECONDS)
    entry = ds_pending.get(ds_sid)
    if entry and entry.get("expire_token") == expire_token:
        ds_pending.pop(ds_sid, None)
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, msg_id)


async def expire_preset_save_later(save_id: str, bot, chat_id: int, ask_msg_id: int, user_id: int):
    """5 分钟无操作：删掉「请输入预设名称」引导消息 + 清除 WAITING_PRESET_NAME 状态。
    保存按钮保留在结果图上，用户回来可以再次点击。"""
    await expire_waiting_flow_later(
        user_id,
        "WAITING_PRESET_NAME",
        "save_id",
        save_id,
        bot,
        chat_id,
        ask_msg_id,
    )


async def expire_pending_preset_save_later(save_id: str, delay_seconds: Optional[int] = None):
    """清理未被保存的提取服装结果缓存。

    保存按钮所在结果图凌晨也会被清理，因此临时文件保留到凌晨即可。
    """
    await asyncio.sleep(delay_seconds if delay_seconds is not None else seconds_until_midnight())
    save_info = pending_preset_saves.pop(save_id, None)
    if save_info:
        cleanup_pending_preset_file(save_info)
        logger.info("待保存图片预设已过期并清理：save_id=%s", save_id)


def build_result_caption(prompt: str, cost_time: str, index: int, total: int) -> str:
    time_str   = f"总耗时：{cost_time}s" if cost_time else ""
    header     = f"✅ 处理完成！{time_str}"
    index_line = f"（图 {index}/{total}）\n" if total > 1 else ""
    footer     = "⏳ 这张图将在凌晨自动清理"
    prefix     = f"{header}\n{index_line}提示词："
    suffix     = f"\n{footer}"
    max_prompt = TG_CAPTION_MAX - len(prefix) - len(suffix) - 1  # -1 留给省略号
    if len(prompt) > max_prompt:
        prompt = prompt[:max_prompt] + "…"
    result = f"{prefix}{prompt}{suffix}"
    if len(result) > TG_CAPTION_MAX:
        result = result[:TG_CAPTION_MAX - 1] + "…"
    return result


async def build_compare_image(orig_bytes: bytes, result_bytes: bytes, left_text: str, right_text: str) -> bytes:
    """拼合原图和结果图为一张对比图，顶部标题栏。返回 PNG bytes。"""
    from PIL import Image, ImageDraw, ImageFont

    orig = Image.open(__import__("io").BytesIO(orig_bytes))
    result = Image.open(__import__("io").BytesIO(result_bytes))
    # 统一高度
    h = max(orig.height, result.height)
    orig = orig.resize((int(orig.width * h / orig.height), h), Image.LANCZOS)
    result = result.resize((int(result.width * h / result.height), h), Image.LANCZOS)
    w = orig.width + result.width
    th = max(56, min(112, w // 12))
    # 尝试加载字体，字号根据图片宽度动态缩放
    font_size = max(40, min(88, w // 12))
    font = None
    for fp in ("/System/Library/Fonts/STHeiti Light.ttc", "/System/Library/Fonts/PingFang.ttc"):
        try:
            font = ImageFont.truetype(fp, font_size)
            break
        except Exception:
            pass
    if font is None:
        font = ImageFont.load_default()
    canvas = Image.new("RGB", (w, h + th), (0, 0, 0))
    draw = ImageDraw.Draw(canvas)
    lb = draw.textbbox((0, 0), left_text, font=font)
    rb = draw.textbbox((0, 0), right_text, font=font)
    draw.text((orig.width // 2 - (lb[2] - lb[0]) // 2, th // 2 - (lb[3] - lb[1]) // 2 - 2), left_text, fill=(255, 255, 255), font=font)
    draw.text((orig.width + result.width // 2 - (rb[2] - rb[0]) // 2, th // 2 - (rb[3] - rb[1]) // 2 - 2), right_text, fill=(255, 255, 255), font=font)
    canvas.paste(orig, (0, th))
    canvas.paste(result, (orig.width, th))
    buf = __import__("io").BytesIO()
    canvas.save(buf, format="PNG")
    return buf.getvalue()


async def delete_message_later(bot, chat_id: int, message_id: int, delay_seconds: int):
    await asyncio.sleep(delay_seconds)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        msg_str = str(e).lower()
        # "message to delete not found" / "message can't be deleted" 是正常现象（已删或已过期），静默忽略
        if "not found" in msg_str or "can't be deleted" in msg_str or "message_id_invalid" in msg_str:
            pass
        else:
            logger.warning("自动删除消息失败（chat=%s msg=%s）：%s", chat_id, message_id, e)


def save_result_to_archive(image_bytes: bytes, user_id: int, index: int = 0):
    """将结果图写入隐藏的生成图存档文件夹。异步安全，直接写不用锁。"""
    try:
        migrate_result_archive_dir()
        RESULT_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%m%d_%H%M%S")
        fname = f"{ts}_u{user_id}_{index}.png"
        (RESULT_ARCHIVE_DIR / fname).write_bytes(image_bytes)
    except Exception:
        logger.exception("存档写入失败")


def cleanup_result_archive():
    """清空隐藏的生成图存档文件夹。"""
    migrate_result_archive_dir()
    if RESULT_ARCHIVE_DIR.exists():
        try:
            shutil.rmtree(RESULT_ARCHIVE_DIR)
            RESULT_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
            logger.info("生成图存档已清空")
        except Exception:
            logger.exception("清空生成图存档失败")


def cleanup_runtime_temp_dirs():
    """清理跨进程重启后遗留的临时会话/预设缓存文件。"""
    for path in (SESSION_IMAGE_DIR, SESSION_VIDEO_DIR, PENDING_PRESET_DIR):
        if path.exists():
            with contextlib.suppress(Exception):
                shutil.rmtree(path)
        with contextlib.suppress(Exception):
            path.mkdir(parents=True, exist_ok=True)


def cleanup_temp_file(path: Optional[str]):
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        logger.exception("清理临时文件失败：%s", path)


async def cleanup_image_preset_file_if_unreferenced(path: Optional[str]):
    if not path:
        return
    data = await load_data()
    target = str(path)
    for user in data.get("users", {}).values():
        if target in (user.get("image_presets") or {}).values():
            return
    cleanup_temp_file(path)


def normalize_voice_preset_info(info) -> dict:
    if isinstance(info, dict):
        return {
            "path": info.get("path") or "",
            "filename": info.get("filename") or Path(info.get("path") or "voice.mp3").name,
            "content_type": normalize_audio_content_type(info.get("content_type"), info.get("filename") or info.get("path")),
        }
    if isinstance(info, str):
        return {
            "path": info,
            "filename": Path(info).name,
            "content_type": audio_content_type_from_path(info),
        }
    return {"path": "", "filename": "voice.mp3", "content_type": DEFAULT_AUDIO_CONTENT_TYPE}


def voice_preset_path(info) -> Optional[str]:
    normalized = normalize_voice_preset_info(info)
    return normalized.get("path") or None


async def cleanup_voice_preset_file_if_unreferenced(path: Optional[str]):
    if not path:
        return
    data = await load_data()
    target = str(path)
    for user in data.get("users", {}).values():
        presets = user.get("voice_presets") or {}
        for preset_info in presets.values():
            if voice_preset_path(preset_info) == target:
                return
    cleanup_temp_file(path)


def build_image_preset_save_path(
    user_id: int,
    preset_name: str,
    content_type: Optional[str],
    filename: Optional[str],
    unique_id: Optional[str] = None,
) -> Path:
    safe_name = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', preset_name).strip("_") or "preset"
    suffix = unique_id or uuid.uuid4().hex[:10]
    save_ext = image_extension_for_save(content_type, filename)
    return PRESET_IMAGE_DIR / str(user_id) / f"{safe_name}-{suffix}{save_ext}"


def build_voice_preset_save_path(
    user_id: int,
    preset_name: str,
    content_type: Optional[str],
    filename: Optional[str],
    unique_id: Optional[str] = None,
) -> Path:
    safe_name = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', preset_name).strip("_") or "voice"
    suffix = unique_id or uuid.uuid4().hex[:10]
    save_ext = audio_extension_for_save(content_type, filename)
    return VOICE_PRESET_DIR / str(user_id) / f"{safe_name}-{suffix}{save_ext}"


async def save_image_preset_file(
    user_id: int,
    preset_name: str,
    image_bytes: bytes,
    image_filename: str,
    image_content_type: str,
    unique_id: Optional[str] = None,
) -> Path:
    PRESET_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    user_preset_dir = PRESET_IMAGE_DIR / str(user_id)
    user_preset_dir.mkdir(parents=True, exist_ok=True)
    save_path = build_image_preset_save_path(
        user_id,
        preset_name,
        image_content_type,
        image_filename,
        unique_id=unique_id,
    )
    await asyncio.to_thread(save_path.write_bytes, image_bytes)

    def _mutate(data: dict):
        user = get_user(data, user_id)
        image_presets = user.setdefault("image_presets", {})
        old_path = image_presets.get(preset_name)
        image_presets[preset_name] = str(save_path)
        return old_path

    old_path = await update_data(_mutate)
    if old_path and old_path != str(save_path):
        await cleanup_image_preset_file_if_unreferenced(old_path)
    return save_path


async def save_voice_preset_file(
    user_id: int,
    preset_name: str,
    audio_bytes: bytes,
    audio_filename: str,
    audio_content_type: str,
    unique_id: Optional[str] = None,
) -> Path:
    VOICE_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    user_preset_dir = VOICE_PRESET_DIR / str(user_id)
    user_preset_dir.mkdir(parents=True, exist_ok=True)
    save_path = build_voice_preset_save_path(
        user_id,
        preset_name,
        audio_content_type,
        audio_filename,
        unique_id=unique_id,
    )
    await asyncio.to_thread(save_path.write_bytes, audio_bytes)

    preset_info = {
        "path": str(save_path),
        "filename": Path(audio_filename).name or save_path.name,
        "content_type": normalize_audio_content_type(audio_content_type, audio_filename),
    }

    def _mutate(data: dict):
        user = get_user(data, user_id)
        voice_presets = user.setdefault("voice_presets", {})
        old_info = voice_presets.get(preset_name)
        voice_presets[preset_name] = preset_info
        return voice_preset_path(old_info)

    old_path = await update_data(_mutate)
    if old_path and old_path != str(save_path):
        await cleanup_voice_preset_file_if_unreferenced(old_path)
    return save_path


def build_session_image_path(
    session_id: str,
    content_type: Optional[str],
    filename: Optional[str],
) -> Path:
    SESSION_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
    return SESSION_IMAGE_DIR / f"{session_id}{image_extension_for_save(content_type, filename)}"


def build_session_video_path(
    session_id: str,
    content_type: Optional[str],
    filename: Optional[str],
) -> Path:
    SESSION_VIDEO_DIR.mkdir(parents=True, exist_ok=True)
    return SESSION_VIDEO_DIR / f"{session_id}{video_extension_for_save(content_type, filename)}"


def build_pending_preset_path(
    save_id: str,
    content_type: Optional[str],
    filename: Optional[str],
) -> Path:
    PENDING_PRESET_DIR.mkdir(parents=True, exist_ok=True)
    return PENDING_PRESET_DIR / f"{save_id}{image_extension_for_save(content_type, filename)}"


async def get_session_image_bytes(session: dict) -> Optional[bytes]:
    image_bytes = session.get("image_bytes")
    if image_bytes:
        return image_bytes
    image_path = session.get("image_path")
    if not image_path:
        return None
    try:
        return await asyncio.to_thread(Path(image_path).read_bytes)
    except Exception:
        logger.exception("读取会话图片失败：%s", image_path)
        return None


async def get_video_outfit_session_bytes(session: dict) -> Optional[bytes]:
    video_bytes = session.get("video_bytes")
    if video_bytes:
        return video_bytes
    video_path = session.get("video_path")
    if not video_path:
        return None
    try:
        return await asyncio.to_thread(Path(video_path).read_bytes)
    except Exception:
        logger.exception("读取会话视频失败：%s", video_path)
        return None


async def read_pending_preset_bytes(save_info: dict) -> Optional[bytes]:
    preset_bytes = save_info.get("bytes")
    if preset_bytes:
        return preset_bytes
    preset_path = save_info.get("path")
    if not preset_path:
        return None
    try:
        return await asyncio.to_thread(Path(preset_path).read_bytes)
    except Exception:
        logger.exception("读取待保存预设图片失败：%s", preset_path)
        return None


def cleanup_pending_preset_file(save_info: Optional[dict]):
    if save_info:
        cleanup_temp_file(save_info.get("path"))


async def handle_application_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    error = context.error
    if isinstance(error, Conflict):
        logger.error("Telegram 冲突：检测到另一个 long-poll 会话，进程退出由后台服务重启……")
        os._exit(75)  # 非 0 → launchd 走异常退出自动拉起。
                      # Conflict 通常是瞬时的（旧 long-poll 在 Telegram 端尚未释放），
                      # 重启一次基本就能恢复；只有用户主动关闭（trap）才不会再被拉起。
                      # 必须用 os._exit 而非 sys.exit：后者在 asyncio Task 内只抛
                      # SystemExit，会被 Task 捕获不传播，进程不会真正退出。

    if isinstance(error, TimedOut) and "Pool timeout" in str(error):
        logger.error("Telegram 请求过密：%s", error)
        with contextlib.suppress(Exception):
            await send_overload_alert(update, context)
        return

    logger.exception("Unhandled Telegram error", exc_info=error)


_last_overload_alert_at = 0.0


async def send_overload_alert(update: object, context: ContextTypes.DEFAULT_TYPE):
    global _last_overload_alert_at
    now = time.time()
    if now - _last_overload_alert_at < 300:
        return
    _last_overload_alert_at = now

    chat_id = None
    reply_to_message_id = None
    if getattr(update, "effective_chat", None):
        chat_id = update.effective_chat.id
    if getattr(update, "effective_message", None):
        reply_to_message_id = update.effective_message.message_id

    if chat_id is None:
        return

    await send_direct_telegram_message(
        chat_id=chat_id,
        text="⚠️ Bot 正在处理较多请求，请稍后再试一次。",
        reply_to_message_id=reply_to_message_id,
    )


async def send_direct_telegram_message(chat_id: int, text: str, reply_to_message_id: Optional[int] = None):
    if not TG_TOKEN:
        return
    payload = {"chat_id": chat_id, "text": text}
    if reply_to_message_id is not None:
        payload["reply_to_message_id"] = reply_to_message_id

    timeout = aiohttp.ClientTimeout(total=8, connect=3, sock_connect=3, sock_read=5)
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=payload) as resp:
            resp.raise_for_status()


# ─── RunningHub API ───────────────────────────

def rh_headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


async def rh_upload_media(
    session: aiohttp.ClientSession,
    api_key: str,
    file_bytes: bytes,
    filename: str,
    content_type: str,
) -> str:
    form = aiohttp.FormData()
    form.add_field("file", file_bytes, filename=filename, content_type=content_type)
    headers = {"Authorization": f"Bearer {api_key}"}
    async with session.post(RH_UPLOAD_URL, data=form, headers=headers) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("上传结果: %s", data)
        root = data["data"] if isinstance(data.get("data"), dict) else data
        return root.get("fileName")


async def rh_upload_image(
    session: aiohttp.ClientSession,
    api_key: str,
    image_bytes: bytes,
    filename: str,
    content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
) -> str:
    return await rh_upload_media(session, api_key, image_bytes, filename, content_type)


async def rh_upload_video(
    session: aiohttp.ClientSession,
    api_key: str,
    video_bytes: bytes,
    filename: str,
    content_type: str = DEFAULT_VIDEO_CONTENT_TYPE,
) -> str:
    return await rh_upload_media(session, api_key, video_bytes, filename, content_type)


async def rh_upload_audio(
    session: aiohttp.ClientSession,
    api_key: str,
    audio_bytes: bytes,
    filename: str,
    content_type: str = DEFAULT_AUDIO_CONTENT_TYPE,
) -> str:
    return await rh_upload_media(session, api_key, audio_bytes, filename, content_type)


def extract_task_id_or_raise(data: dict, label: str) -> str:
    root = data.get("data") if isinstance(data.get("data"), dict) else data
    if not isinstance(root, dict):
        raise RuntimeError(f"{label} API 返回格式异常：{data!r}")

    task_id = root.get("taskId") or root.get("taskID") or root.get("task_id")
    if task_id:
        return task_id

    error_code = (
        root.get("errorCode")
        or root.get("code")
        or data.get("errorCode")
        or data.get("code")
        or "unknown"
    )
    error_message = (
        root.get("errorMessage")
        or root.get("msg")
        or data.get("errorMessage")
        or data.get("msg")
        or "unknown"
    )
    raise RuntimeError(f"{label} API 返回异常：errorCode={error_code}，message={error_message}")


async def rh_run_workflow(
    session: aiohttp.ClientSession,
    api_key: str,
    file_name: str,
    prompt: str,
    workflow_key: str = DEFAULT_CUSTOM_WORKFLOW_KEY,
) -> str:
    spec = WORKFLOWS[workflow_key]
    node_list = build_node_info_list(
        spec,
        {
            "image": file_name,
            "prompt": prompt,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_faceswap(
    session: aiohttp.ClientSession,
    api_key: str,
    orig_file_name: str,
    face_file_name: str,
) -> str:
    spec = WORKFLOWS["faceswap"]
    node_list = build_node_info_list(
        spec,
        {
            "original_image": orig_file_name,
            "face_image": face_file_name,
        },
    )
    async with session.post(spec.endpoint, json={"nodeInfoList": node_list}, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_outfit(
    session: aiohttp.ClientSession,
    api_key: str,
    file_name: str,
) -> str:
    spec = WORKFLOWS["outfit_extract"]
    node_list = build_node_info_list(spec, {"image": file_name})
    async with session.post(spec.endpoint, json={"nodeInfoList": node_list}, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_bodyswap(
    session: aiohttp.ClientSession,
    api_key: str,
    orig_file_name: str,
    ref_file_name: str,
) -> str:
    spec = WORKFLOWS["firered"]
    node_list = build_node_info_list(
        spec,
        {
            "original_image": orig_file_name,
            "reference_image": ref_file_name,
        },
    )
    async with session.post(spec.endpoint, json={"nodeInfoList": node_list}, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_qwen_outfit(
    session: aiohttp.ClientSession,
    api_key: str,
    orig_file_name: str,
    ref_file_name: str,
) -> str:
    spec = WORKFLOWS["qwen"]
    node_list = build_node_info_list(
        spec,
        {
            "original_image": orig_file_name,
            "reference_image": ref_file_name,
        },
    )
    async with session.post(spec.endpoint, json={"nodeInfoList": node_list}, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_scene_replace(
    session: aiohttp.ClientSession,
    api_key: str,
    scene_file_name: str,
    person_file_name: str,
) -> str:
    spec = WORKFLOWS["scene_replace"]
    node_list = build_node_info_list(
        spec,
        {
            "scene_image": scene_file_name,
            "person_image": person_file_name,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_image_extend(
    session: aiohttp.ClientSession,
    api_key: str,
    file_name: str,
    extend_values: dict[str, int],
) -> str:
    spec = WORKFLOWS["image_expand"]
    node_list = build_node_info_list(
        spec,
        {
            "image": file_name,
            "top": extend_values.get("top", IMAGE_EXTEND_PIXELS),
            "bottom": extend_values.get("bottom", IMAGE_EXTEND_PIXELS),
            "right": extend_values.get("right", IMAGE_EXTEND_PIXELS),
            "left": extend_values.get("left", IMAGE_EXTEND_PIXELS),
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_image_animation(
    session: aiohttp.ClientSession,
    api_key: str,
    file_name: str,
    prompt: str = DEFAULT_ANIMATION_PROMPT,
    seconds: int = DEFAULT_ANIMATION_SECONDS,
) -> str:
    spec = WORKFLOWS["image_animation"]
    node_list = build_node_info_list(
        spec,
        {
            "image": file_name,
            "prompt": prompt,
            "time": seconds,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_video_outfit(
    session: aiohttp.ClientSession,
    api_key: str,
    video_file_name: str,
    ref_file_name: str,
) -> str:
    spec = WORKFLOWS["video_outfit"]
    node_list = build_node_info_list(
        spec,
        {
            "video": video_file_name,
            "reference_image": ref_file_name,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_first_last_video(
    session: aiohttp.ClientSession,
    api_key: str,
    first_file_name: str,
    last_file_name: str,
    max_side: int,
    seconds: int = FIRST_LAST_VIDEO_SECONDS,
    prompt: str = DEFAULT_FIRST_LAST_VIDEO_PROMPT,
) -> str:
    spec = WORKFLOWS["first_last_video"]
    node_list = build_node_info_list(
        spec,
        {
            "first_image": first_file_name,
            "last_image": last_file_name,
            "max_side": max_side,
            "time": seconds,
            "prompt": prompt,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_run_talking_video(
    session: aiohttp.ClientSession,
    api_key: str,
    image_file_name: str,
    audio_file_name: str,
    seconds: int,
    prompt: str,
) -> str:
    spec = WORKFLOWS["talking_video"]
    node_list = build_node_info_list(
        spec,
        {
            "image": image_file_name,
            "audio": audio_file_name,
            "time": seconds,
            "prompt": prompt,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


def get_voice_clone_workflow_config_error() -> Optional[str]:
    spec = WORKFLOWS.get("voice_clone")
    if not spec:
        return "未找到 voice_clone 工作流配置"
    if not (spec.endpoint or "").startswith("http"):
        return (
            "还没配置声音克隆工作流。请在 .env、环境变量或 bot_secrets.py 里设置 RH_VOICE_CLONE_ENDPOINT，"
            "以及必要时设置 RH_VOICE_SAMPLE_NODE_ID/RH_VOICE_SAMPLE_FIELD/"
            "RH_VOICE_TEXT_NODE_ID/RH_VOICE_TEXT_FIELD。"
        )
    sample_node = spec.nodes.get("sample_audio")
    text_node = spec.nodes.get("text")
    if not sample_node or not sample_node[0] or not sample_node[1] or not text_node or not text_node[0] or not text_node[1]:
        return "声音克隆工作流的音频/文案节点配置不完整。"
    return None


async def rh_run_voice_clone(
    session: aiohttp.ClientSession,
    api_key: str,
    sample_file_name: str,
    text: str,
) -> str:
    config_error = get_voice_clone_workflow_config_error()
    if config_error:
        raise RuntimeError(config_error)
    spec = WORKFLOWS["voice_clone"]
    node_list = build_node_info_list(
        spec,
        {
            "sample_audio": sample_file_name,
            "text": text,
        },
    )
    payload = {
        "nodeInfoList": node_list,
        "instanceType": "default",
        "usePersonalQueue": "false",
    }
    async with session.post(spec.endpoint, json=payload, headers=rh_headers(api_key)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        logger.info("%s 工作流触发结果: %s", spec.label, data)
        return extract_task_id_or_raise(data, spec.label)


async def rh_poll_result(
    session: aiohttp.ClientSession,
    api_key: str,
    task_id: str,
    on_tick=None,
    tick_interval: int = 10,  # 每隔多少秒更新一次进度消息（避免 Telegram 限速）
) -> tuple[list[str], str]:
    """返回 (result_urls, taskCostTime)。on_tick(elapsed_s) 每 tick_interval 秒调用一次。"""
    payload    = {"taskId": task_id}
    deadline   = time.time() + POLL_TIMEOUT
    start_time = time.time()
    last_tick  = -tick_interval  # 保证第一次立即触发

    while time.time() < deadline:
        async with session.post(RH_QUERY_URL, json=payload, headers=rh_headers(api_key)) as resp:
            resp.raise_for_status()
            data = await resp.json()
            logger.debug("轮询结果: %s", data)

            root    = data["data"] if isinstance(data.get("data"), dict) else data
            status  = root.get("status")
            results = root.get("results")

            if status is None:
                error_code = (
                    root.get("errorCode")
                    or root.get("code")
                    or data.get("errorCode")
                    or data.get("code")
                    or "unknown"
                )
                error_message = (
                    root.get("errorMessage")
                    or root.get("msg")
                    or data.get("errorMessage")
                    or data.get("msg")
                    or "unknown"
                )
                raise RuntimeError(f"查询 API 返回异常：errorCode={error_code}，message={error_message}")

            if status is not None:
                status_upper = str(status).upper()
                if status_upper in ("SUCCESS", "COMPLETED", "FINISH"):
                    urls = []
                    if results:
                        for item in (results if isinstance(results, list) else [results]):
                            url = item.get("url") if isinstance(item, dict) else str(item)
                            if url:
                                urls.append(url)
                    unique_urls = dedupe_preserving_order(urls)
                    if len(unique_urls) != len(urls):
                        logger.warning(
                            "任务 %s 返回了重复结果 URL，已去重：%d -> %d",
                            task_id, len(urls), len(unique_urls),
                        )
                    logger.info("任务 %s 完成，返回 %d 个结果", task_id, len(unique_urls))
                    usage = root.get("usage") if isinstance(root.get("usage"), dict) else {}
                    cost_time = str(root.get("taskCostTime") or usage.get("taskCostTime") or "")
                    if not cost_time and results:
                        first = (results if isinstance(results, list) else [results])[0]
                        cost_time = str(first.get("taskCostTime") or "") if isinstance(first, dict) else ""
                    return unique_urls, cost_time

                if status_upper in ("FAILED", "ERROR"):
                    failed_reason = root.get("failedReason")
                    error_message = root.get("errorMessage") or root.get("msg") or data.get("errorMessage") or data.get("msg")
                    detail = error_message or failed_reason
                    if detail:
                        raise RuntimeError(f"工作流执行失败，状态: {status}，原因：{detail}")
                    raise RuntimeError(f"工作流执行失败，状态: {status}")

                if status_upper not in ("PENDING", "PROCESSING", "RUNNING", "QUEUED"):
                    logger.warning("任务 %s 未知状态: %s，快速失败", task_id, status)
                    raise RuntimeError(f"工作流返回未知状态: {status}")

            logger.debug("任务 %s 状态: %s，继续等待…", task_id, status)

        elapsed = int(time.time() - start_time)
        if on_tick and elapsed - last_tick >= tick_interval:
            last_tick = elapsed
            with contextlib.suppress(Exception):
                await on_tick(elapsed)

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(f"任务 {task_id} 超时（{POLL_TIMEOUT}s 内未完成）")


async def download_url(session: aiohttp.ClientSession, url: str) -> bytes:
    # 结果图/视频可能较大，媒体下载统一给 20 分钟。
    async with session.get(
        url,
        timeout=aiohttp.ClientTimeout(
            total=MEDIA_TRANSFER_TIMEOUT,
            connect=10,
            sock_read=MEDIA_TRANSFER_TIMEOUT,
        ),
    ) as resp:
        resp.raise_for_status()
        return await resp.read()


async def ds_generate_prompt(user_id: int, user_system_prompt: Optional[str] = None) -> str:
    """调用 DeepSeek 生成一条图像处理提示词。
    优先级：user_system_prompt > deepseek_prompt.txt > 极简兜底。
    携带当天对话历史，避免重复生成相同主题。
    """
    if not DS_API_KEY:
        raise RuntimeError("未配置 DeepSeek API Key，AI 随机风格不可用；其他工作流不受影响。")

    system_prompt = user_system_prompt or DS_SYSTEM_PROMPT

    # history = ds_histories.get(user_id, [])  # 暂时关闭上下文记忆
    messages = [{"role": "system", "content": system_prompt}]
    # messages.extend(history)
    messages.append({"role": "user", "content": DS_GENERATE_USER_MESSAGE})

    payload = {
        "model": DS_MODEL,
        "messages": messages,
        "max_tokens": DS_MAX_TOKENS,
        "temperature": 1.2,
    }
    headers = {
        "Authorization": f"Bearer {DS_API_KEY}",
        "Content-Type": "application/json",
    }
    http_session = await get_shared_http_session()
    last_error: Optional[Exception] = None
    for attempt in range(1, DS_GENERATE_RETRIES + 1):
        try:
            async with http_session.post(
                DS_API_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=45),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
            generated = _extract_ds_prompt_from_response(data)
            if attempt > 1:
                logger.info("DeepSeek 第 %s 次重试后生成成功（user=%s，chars=%s）", attempt - 1, user_id, len(generated))
            return generated
        except aiohttp.ClientResponseError as e:
            if e.status < 500 and e.status != 429:
                raise RuntimeError(f"DeepSeek API 请求失败：HTTP {e.status}") from e
            last_error = e
        except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as e:
            last_error = e

        logger.warning(
            "DeepSeek 生成提示词失败（user=%s，attempt=%s/%s）：%s",
            user_id,
            attempt,
            DS_GENERATE_RETRIES,
            last_error,
        )
        if attempt < DS_GENERATE_RETRIES:
            await asyncio.sleep(0.6 * attempt)

    raise RuntimeError(f"DeepSeek 连续 {DS_GENERATE_RETRIES} 次没有返回完整提示词：{last_error}") from last_error



# ─── 图像处理主流程（全程 async，多用户并发互不阻塞）─────

async def _make_compare_bytes(user_id: int, image_bytes: bytes, result_bytes: bytes, switch_key: str) -> Optional[bytes]:
    """如果用户开启了对比图开关，返回对比图 bytes；否则返回 None。"""
    data = await load_data()
    user = get_user(data, user_id)
    switches = user.get("compare_switches", {})
    if not switches.get(switch_key):
        return None
    left_text = user.get("compare_origin_text", "").strip()
    right_text = user.get("compare_result_text", "").strip()
    # 如果文案为空，使用默认值
    if not left_text:
        left_text = "原图"
    if not right_text:
        right_text = "输出图"
    try:
        return await build_compare_image(image_bytes, result_bytes, left_text, right_text)
    except Exception:
        logger.exception("生成对比图失败")
        return None


async def _send_result_with_compare(msg, result_bytes: bytes, filename: str, caption: str, compare_bytes: Optional[bytes], chat_id: int, user_id: int):
    """发送结果图 + 对比图（可选），以文件模式逐一发送以提高成功率。"""
    if compare_bytes:
        result_msg = await reply_document_with_fallback(
            msg,
            document=result_bytes,
            filename=filename,
            caption=caption,
        )
        compare_msg = await reply_document_with_fallback(
            msg,
            document=compare_bytes,
            filename="compare.png",
            caption="🪞 对比图",
        )
        return [result_msg, compare_msg]
    result_msg = await reply_document_with_fallback(msg, document=result_bytes, filename=filename, caption=caption)
    return [result_msg]


async def _maybe_send_compare(msg, user_id: int, image_bytes: bytes, result_bytes: bytes, switch_key: str):
    """已废弃：保留用于兼容，实际走 _make_compare_bytes + _send_result_with_compare。"""
    compare_bytes = await _make_compare_bytes(user_id, image_bytes, result_bytes, switch_key)
    if compare_bytes:
        compare_msg = await reply_document_with_fallback(
            msg,
            document=compare_bytes,
            filename="compare.png",
            caption="🪞 对比图",
        )
        await register_for_cleanup(compare_msg.chat_id, compare_msg.message_id)


async def process_image(
    msg,
    api_key: str,
    image_bytes: bytes,
    prompt: str,
    tg_user=None,
    display_prompt: Optional[str] = None,
    delete_on_success: Optional[list] = None,
    image_filename: str = "input.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        # 上传
        file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not file_name:
            raise ValueError("上传后未能获取 fileName，请检查 API 响应")
        logger.info("上传成功，fileName: %s", file_name)

        # 触发工作流
        prompt_for_display = display_prompt or prompt
        await safe_edit_text(status_msg, build_processing_start_message(prompt_for_display))
        workflow_start = time.time()
        task_id = await rh_run_workflow(session, api_key, file_name, prompt)
        if not task_id:
            raise ValueError("未能获取 taskId，请检查工作流 API 响应")
        logger.info("任务已创建，taskId: %s", task_id)

        # 轮询结果
        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_image(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_image)
        result_urls = dedupe_preserving_order(result_urls)
        # API 没返回耗时时，用本地计时兜底
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片，请检查工作流输出节点配置。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, prompt, cost_time)

        # 回传结果（解码后优先，fallback 原图）
        await safe_edit_text(status_msg, "📤 回传中…")
        for i, url in enumerate(result_urls):
            logger.info("结果图片 %d: %s", i + 1, url)
            raw_bytes = await download_url(session, url)

            caption  = build_result_caption(prompt_for_display, cost_time, i + 1, len(result_urls))
            filename = build_result_filename(i, len(result_urls))

            # 原生 Python 解码，在线程池执行避免阻塞事件循环
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            if decoded_bytes:
                send_bytes = decoded_bytes
                logger.info("解码成功，发送解码图（图 %d）", i + 1)
            else:
                send_bytes = raw_bytes
                caption   += "\n⚠️ 解码失败，发送原图"
                logger.warning("解码失败，fallback 发送原图（图 %d）", i + 1)

            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)

            # 对比图（仅第一张）- custom 开关控制
            if i == 0:
                tg_id = (tg_user or msg.from_user).id
                compare_bytes = await _make_compare_bytes(tg_id, image_bytes, send_bytes, "custom")
                if compare_bytes:
                    result_messages = await _send_result_with_compare(msg, send_bytes, filename, caption, compare_bytes, msg.chat_id, tg_id)
                    for rm in result_messages:
                        await register_for_cleanup(rm.chat_id, rm.message_id)
                else:
                    result_message = await reply_document_with_fallback(
                        msg,
                        document=send_bytes,
                        filename=filename,
                        caption=caption,
                    )
                    await register_for_cleanup(result_message.chat_id, result_message.message_id)
            else:
                result_message = await reply_document_with_fallback(
                    msg,
                    document=send_bytes,
                    filename=filename,
                    caption=caption,
                )
                await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        # 自定义提示词路径：生成成功后立即清除"请输入提示词"和用户输入
        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_faceswap(
    msg,
    api_key: str,
    image_bytes: bytes,
    face_bytes: bytes,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    image_filename: str = "original.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    face_filename: str = "face.jpg",
    face_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        # 上传原图和脸图
        orig_file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not orig_file_name:
            raise ValueError("原图上传后未能获取 fileName")
        face_file_name = await rh_upload_image(session, api_key, face_bytes, face_filename, face_content_type)
        if not face_file_name:
            raise ValueError("脸图上传后未能获取 fileName")

        # 触发换脸工作流
        await safe_edit_text(status_msg, "⚙️ 换脸处理中…")
        workflow_start = time.time()
        task_id = await rh_run_faceswap(session, api_key, orig_file_name, face_file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("换脸任务已创建，taskId: %s", task_id)

        # 轮询结果
        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_faceswap(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_faceswap)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "换脸", cost_time)

        # 回传结果（优先本地解码，失败则发送原图）
        await safe_edit_text(status_msg, "📤 回传中…")
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            if decoded_bytes:
                send_bytes = decoded_bytes
                logger.info("换脸结果解码成功，发送解码图（图 %d）", i + 1)
            else:
                send_bytes = raw_bytes
                logger.warning("换脸结果解码失败，fallback 发送原图（图 %d）", i + 1)
            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)
            caption = (
                f"✅ 换脸完成！总耗时：{cost_time}s\n"
                f"⏳ 这张图将在凌晨自动清理"
            )
            filename = build_result_filename(i, len(result_urls))
            # 对比图
            if i == 0:
                tg_id = (tg_user or msg.from_user).id
                compare_bytes = await _make_compare_bytes(tg_id, image_bytes, send_bytes, "faceswap")
                if compare_bytes:
                    result_messages = await _send_result_with_compare(msg, send_bytes, filename, caption, compare_bytes, msg.chat_id, tg_id)
                    for rm in result_messages:
                        await register_for_cleanup(rm.chat_id, rm.message_id)
                else:
                    result_message = await reply_document_with_fallback(
                        msg,
                        document=send_bytes,
                        filename=filename,
                        caption=caption,
                    )
                    await register_for_cleanup(result_message.chat_id, result_message.message_id)
            else:
                result_message = await reply_document_with_fallback(
                    msg,
                    document=send_bytes,
                    filename=filename,
                    caption=caption,
                )
                await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("换脸处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_scene_replace(
    msg,
    api_key: str,
    scene_bytes: bytes,
    person_bytes: bytes,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    scene_filename: str = "scene.jpg",
    scene_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    person_filename: str = "person.jpg",
    person_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        scene_file_name = await rh_upload_image(session, api_key, scene_bytes, scene_filename, scene_content_type)
        if not scene_file_name:
            raise ValueError("场景图上传后未能获取 fileName")
        person_file_name = await rh_upload_image(session, api_key, person_bytes, person_filename, person_content_type)
        if not person_file_name:
            raise ValueError("人物图上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 场景换人处理中…")
        workflow_start = time.time()
        task_id = await rh_run_scene_replace(session, api_key, scene_file_name, person_file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("场景换人任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_scene_replace(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_scene_replace)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "场景换人", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            if decoded_bytes:
                send_bytes = decoded_bytes
                logger.info("场景换人结果解码成功，发送解码图（图 %d）", i + 1)
            else:
                send_bytes = raw_bytes
                logger.warning("场景换人结果解码失败，fallback 发送原图（图 %d）", i + 1)
            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)
            caption = (
                f"✅ 场景换人完成！总耗时：{cost_time}s\n"
                f"⏳ 这张图将在凌晨自动清理"
            )
            result_message = await reply_document_with_fallback(
                msg,
                document=send_bytes,
                filename=build_result_filename(i, len(result_urls)),
                caption=caption,
            )
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("场景换人超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("场景换人处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_outfit(
    msg,
    api_key: str,
    image_bytes: bytes,
    user_id: int,
    tg_user=None,
    image_filename: str = "outfit_input.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    """提取图片服装并返回结果，附带保存预设按钮。"""
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return
    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not file_name:
            raise ValueError("上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 提取服装中…")
        workflow_start = time.time()
        task_id = await rh_run_outfit(session, api_key, file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_outfit(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_outfit)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "提取服装", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)

            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            if decoded_bytes:
                send_bytes = decoded_bytes
            else:
                send_bytes = raw_bytes

            save_result_to_archive(send_bytes, user_id, i)

            save_id = uuid.uuid4().hex[:10]
            filename = build_result_filename(i, len(result_urls))
            pending_path = build_pending_preset_path(
                save_id,
                image_content_type_from_bytes(send_bytes),
                filename,
            )
            try:
                await asyncio.to_thread(pending_path.write_bytes, send_bytes)
                save_keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("💾 保存为预设", callback_data=f"outfit:save:{save_id}"),
                ]])
                caption = (
                    f"✅ 服装提取完成！总耗时：{cost_time}s\n"
                    f"⏳ 这张图将在凌晨自动清理"
                )
                result_message = await reply_document_with_fallback(
                    msg,
                    document=send_bytes,
                    filename=filename,
                    caption=caption,
                    reply_markup=save_keyboard,
                )
            except Exception:
                cleanup_temp_file(str(pending_path))
                raise
            await register_for_cleanup(result_message.chat_id, result_message.message_id)
            pending_preset_saves[save_id] = {
                "path": str(pending_path),
                "user_id": user_id,
                "filename": filename,
                "content_type": image_content_type_from_bytes(send_bytes),
                "result_msg_id": result_message.message_id,
                "chat_id": result_message.chat_id,
            }
            schedule_background_task(expire_pending_preset_save_later(save_id))

        await safe_delete_message(status_msg)

    except TimeoutError as e:
        logger.error("超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("提取服装处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_bodyswap(
    msg,
    api_key: str,
    image_bytes: bytes,
    ref_bytes: bytes,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    image_filename: str = "original.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    ref_filename: str = "reference.jpg",
    ref_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    """参考图换身体：原图 + 参考图 → bodyswap 工作流。"""
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        orig_file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not orig_file_name:
            raise ValueError("原图上传后未能获取 fileName")
        ref_file_name = await rh_upload_image(session, api_key, ref_bytes, ref_filename, ref_content_type)
        if not ref_file_name:
            raise ValueError("参考图上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 参考换衣（红火）处理中…")
        workflow_start = time.time()
        task_id = await rh_run_bodyswap(session, api_key, orig_file_name, ref_file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("换身体任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_bodyswap(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_bodyswap)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "参考换衣（红火）", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            if decoded_bytes:
                send_bytes = decoded_bytes
            else:
                send_bytes = raw_bytes
            caption = (
                f"✅ 参考换衣（红火）完成！总耗时：{cost_time}s\n"
                f"⏳ 这张图将在凌晨自动清理"
            )
            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)
            filename = build_result_filename(i, len(result_urls))
            # 对比图
            if i == 0:
                tg_id = (tg_user or msg.from_user).id
                compare_bytes = await _make_compare_bytes(tg_id, image_bytes, send_bytes, "bodyswap")
                if compare_bytes:
                    result_messages = await _send_result_with_compare(msg, send_bytes, filename, caption, compare_bytes, msg.chat_id, tg_id)
                    for rm in result_messages:
                        await register_for_cleanup(rm.chat_id, rm.message_id)
                else:
                    result_message = await reply_document_with_fallback(
                        msg,
                        document=send_bytes,
                        filename=filename,
                        caption=caption,
                    )
                    await register_for_cleanup(result_message.chat_id, result_message.message_id)
            else:
                result_message = await reply_document_with_fallback(
                    msg,
                    document=send_bytes,
                    filename=filename,
                    caption=caption,
                )
                await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("换身体处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_qwen_outfit(
    msg,
    api_key: str,
    image_bytes: bytes,
    ref_bytes: bytes,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    image_filename: str = "original.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    ref_filename: str = "reference.jpg",
    ref_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    """qwen换装：原图 + 参考服装图 → qwen换装工作流。"""
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        orig_file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not orig_file_name:
            raise ValueError("原图上传后未能获取 fileName")
        ref_file_name = await rh_upload_image(session, api_key, ref_bytes, ref_filename, ref_content_type)
        if not ref_file_name:
            raise ValueError("参考图上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 参考换衣（千问）处理中…")
        workflow_start = time.time()
        task_id = await rh_run_qwen_outfit(session, api_key, orig_file_name, ref_file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("qwen换装任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_qwen(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_qwen)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "参考换衣（千问）", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            send_bytes = decoded_bytes if decoded_bytes else raw_bytes
            caption = (
                f"✅ 参考换衣（千问）完成！总耗时：{cost_time}s\n"
                f"⏳ 这张图将在凌晨自动清理"
            )
            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)
            filename = build_result_filename(i, len(result_urls))
            # 对比图
            if i == 0:
                tg_id = (tg_user or msg.from_user).id
                compare_bytes = await _make_compare_bytes(tg_id, image_bytes, send_bytes, "bodyswap")
                if compare_bytes:
                    result_messages = await _send_result_with_compare(msg, send_bytes, filename, caption, compare_bytes, msg.chat_id, tg_id)
                    for rm in result_messages:
                        await register_for_cleanup(rm.chat_id, rm.message_id)
                else:
                    result_message = await reply_document_with_fallback(
                        msg,
                        document=send_bytes,
                        filename=filename,
                        caption=caption,
                    )
                    await register_for_cleanup(result_message.chat_id, result_message.message_id)
            else:
                result_message = await reply_document_with_fallback(
                    msg,
                    document=send_bytes,
                    filename=filename,
                    caption=caption,
                )
                await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("参考换衣（千问）处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_image_extend(
    msg,
    api_key: str,
    image_bytes: bytes,
    extend_values: dict[str, int],
    tg_user=None,
    image_filename: str = "extend_input.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not file_name:
            raise ValueError("上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 图片扩展处理中…")
        workflow_start = time.time()
        task_id = await rh_run_image_extend(session, api_key, file_name, extend_values)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("图片扩展任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_extend(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_extend)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回图片。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        direction_text = " ".join(
            f"{IMAGE_EXTEND_LABELS[direction]}{extend_values.get(direction, 0)}px"
            for direction in IMAGE_EXTEND_DIRECTIONS
        )
        await log_usage(tg_user or msg.from_user, api_key, f"图片扩展 {direction_text}", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_bytes = await asyncio.to_thread(decode_duck_image, raw_bytes)
            send_bytes = decoded_bytes if decoded_bytes else raw_bytes
            save_result_to_archive(send_bytes, (tg_user or msg.from_user).id, i)
            caption = (
                f"✅ 图片扩展完成！总耗时：{cost_time}s\n"
                f"扩展：{direction_text}\n"
                f"⏳ 这张图将在凌晨自动清理"
            )
            result_message = await reply_document_with_fallback(
                msg,
                document=send_bytes,
                filename=build_result_filename(i, len(result_urls)),
                caption=caption,
            )
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

    except TimeoutError as e:
        logger.error("图片扩展超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("图片扩展处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_image_animation(
    msg,
    api_key: str,
    image_bytes: bytes,
    prompt: str,
    seconds: int = DEFAULT_ANIMATION_SECONDS,
    tg_user=None,
    image_filename: str = "animation_input.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not file_name:
            raise ValueError("上传后未能获取 fileName")

        await safe_edit_text(status_msg, build_processing_start_message(prompt))
        workflow_start = time.time()
        task_id = await rh_run_image_animation(session, api_key, file_name, prompt=prompt, seconds=seconds)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("生成动图任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_animation(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_animation)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回动图。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, f"生成动图（{seconds}s）：{prompt}", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_media = await asyncio.to_thread(decode_duck_media, raw_bytes)
            if decoded_media:
                send_bytes, result_ext = decoded_media
            else:
                send_bytes = raw_bytes
                result_ext = None
            source_token = uuid.uuid4().hex[:12]
            source_ext = normalize_video_outfit_extension(url, result_ext)
            source_path = build_video_outfit_source_path(source_token, source_ext)
            preview_filename = build_animation_filename(url, i, len(result_urls), result_ext)
            source_filename = f"AnimationSource_{i + 1}_of_{len(result_urls)}.{source_ext}" if len(result_urls) > 1 else f"AnimationSource.{source_ext}"
            caption = (
                f"✅ 动图生成完成！总耗时：{cost_time}s\n"
                f"时长：{seconds}s\n"
                f"⏳ 这个文件将在凌晨自动清理"
            )
            try:
                await asyncio.to_thread(source_path.write_bytes, send_bytes)
                register_pending_video_outfit_source(
                    source_token,
                    msg.from_user.id,
                    msg.chat_id,
                    str(source_path),
                    source_filename,
                )
                result_message = await reply_media_with_document_for_images(
                    msg,
                    media=send_bytes,
                    filename=preview_filename,
                    caption=caption,
                    extension=source_ext,
                    reply_markup=build_video_outfit_source_keyboard(source_token),
                )
            except Exception:
                cleanup_pending_video_outfit_source(source_token, {"path": str(source_path)})
                raise
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

    except TimeoutError as e:
        logger.error("生成动图超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("生成动图处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_video_outfit(
    msg,
    api_key: str,
    video_bytes: bytes,
    ref_bytes: bytes,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    video_filename: str = "input.mp4",
    video_content_type: str = DEFAULT_VIDEO_CONTENT_TYPE,
    ref_filename: str = "reference.jpg",
    ref_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    video_session_id: Optional[str] = None,
    video_session: Optional[dict] = None,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    success = False
    try:
        session = await get_shared_http_session()
        video_file_name = await rh_upload_video(session, api_key, video_bytes, video_filename, video_content_type)
        if not video_file_name:
            raise ValueError("视频上传后未能获取 fileName")
        ref_file_name = await rh_upload_image(session, api_key, ref_bytes, ref_filename, ref_content_type)
        if not ref_file_name:
            raise ValueError("参考图上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 视频换衣处理中…")
        workflow_start = time.time()
        task_id = await rh_run_video_outfit(session, api_key, video_file_name, ref_file_name)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("视频换衣任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_video_outfit(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_video_outfit)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回视频。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, "视频换衣", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_media = await asyncio.to_thread(decode_duck_media, raw_bytes)
            if decoded_media:
                send_bytes, result_ext = decoded_media
            else:
                send_bytes = raw_bytes
                result_ext = None
            source_token = uuid.uuid4().hex[:12]
            source_ext = normalize_video_outfit_extension(url, result_ext)
            source_path = build_video_outfit_source_path(source_token, source_ext)
            source_filename = build_video_outfit_source_filename(url, i, len(result_urls), result_ext)
            preview_filename = build_video_outfit_filename(url, i, len(result_urls), result_ext)
            caption = (
                f"✅ 视频换衣完成！总耗时：{cost_time}s\n"
                f"⏳ 这个文件将在凌晨自动清理"
            )
            try:
                await asyncio.to_thread(source_path.write_bytes, send_bytes)
                register_pending_video_outfit_source(
                    source_token,
                    msg.from_user.id,
                    msg.chat_id,
                    str(source_path),
                    source_filename,
                )
                result_message = await reply_media_with_document_for_images(
                    msg,
                    media=send_bytes,
                    filename=preview_filename,
                    caption=caption,
                    extension=source_ext,
                    reply_markup=build_video_outfit_source_keyboard(source_token),
                )
            except Exception:
                cleanup_pending_video_outfit_source(source_token, {"path": str(source_path)})
                raise
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)
        success = True

    except TimeoutError as e:
        logger.error("视频换衣超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("视频换衣处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    finally:
        if video_session_id:
            if success:
                cleanup_video_outfit_session(video_session_id, video_session)
            else:
                session_info = pending_video_outfit_sessions.get(video_session_id)
                if session_info:
                    session_info["processing"] = False


async def process_first_last_video(
    msg,
    api_key: str,
    first_image_bytes: bytes,
    last_image_bytes: bytes,
    seconds: int = FIRST_LAST_VIDEO_SECONDS,
    prompt: str = DEFAULT_FIRST_LAST_VIDEO_PROMPT,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    first_filename: str = "first.jpg",
    first_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    last_filename: str = "last.jpg",
    last_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        max_side = FIRST_LAST_VIDEO_FALLBACK_MAX_SIDE

        session = await get_shared_http_session()
        first_file_name = await rh_upload_image(session, api_key, first_image_bytes, first_filename, first_content_type)
        if not first_file_name:
            raise ValueError("首帧上传后未能获取 fileName")
        last_file_name = await rh_upload_image(session, api_key, last_image_bytes, last_filename, last_content_type)
        if not last_file_name:
            raise ValueError("尾帧上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 首尾视频处理中…")
        workflow_start = time.time()
        task_id = await rh_run_first_last_video(
            session, api_key, first_file_name, last_file_name, max_side, seconds=seconds, prompt=prompt
        )
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("首尾视频任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_first_last(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_first_last)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回视频。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(
            tg_user or msg.from_user,
            api_key,
            f"首尾视频（{seconds}s, max={max_side}）：{prompt[:30]}",
            cost_time,
        )

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_media = await asyncio.to_thread(decode_duck_media, raw_bytes)
            if decoded_media:
                send_bytes, result_ext = decoded_media
            else:
                send_bytes = raw_bytes
                result_ext = None
            source_token = uuid.uuid4().hex[:12]
            source_ext = normalize_video_outfit_extension(url, result_ext)
            source_path = build_video_outfit_source_path(source_token, source_ext)
            preview_filename = build_first_last_video_filename(url, i, len(result_urls), result_ext)
            source_filename = f"FirstLastVideoSource_{i + 1}_of_{len(result_urls)}.{source_ext}" if len(result_urls) > 1 else f"FirstLastVideoSource.{source_ext}"
            caption = (
                f"✅ 首尾视频完成！总耗时：{cost_time}s\n"
                f"时长：{seconds}s\n"
                f"⏳ 这个文件将在凌晨自动清理"
            )
            try:
                await asyncio.to_thread(source_path.write_bytes, send_bytes)
                register_pending_video_outfit_source(
                    source_token,
                    msg.from_user.id,
                    msg.chat_id,
                    str(source_path),
                    source_filename,
                )
                result_message = await reply_media_with_document_for_images(
                    msg,
                    media=send_bytes,
                    filename=preview_filename,
                    caption=caption,
                    extension=source_ext,
                    reply_markup=build_video_outfit_source_keyboard(source_token),
                )
            except Exception:
                cleanup_pending_video_outfit_source(source_token, {"path": str(source_path)})
                raise
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("首尾视频超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("首尾视频处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_talking_video(
    msg,
    api_key: str,
    image_bytes: bytes,
    audio_bytes: bytes,
    seconds: int,
    prompt: str,
    tg_user=None,
    image_filename: str = "portrait.jpg",
    image_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
    audio_filename: str = "speech.mp3",
    audio_content_type: str = DEFAULT_AUDIO_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        image_file_name = await rh_upload_image(session, api_key, image_bytes, image_filename, image_content_type)
        if not image_file_name:
            raise ValueError("图片上传后未能获取 fileName")
        audio_file_name = await rh_upload_audio(session, api_key, audio_bytes, audio_filename, audio_content_type)
        if not audio_file_name:
            raise ValueError("音频上传后未能获取 fileName")

        await safe_edit_text(status_msg, "⚙️ 说话视频处理中…")
        workflow_start = time.time()
        task_id = await rh_run_talking_video(session, api_key, image_file_name, audio_file_name, seconds, prompt)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("说话视频任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_talking_video(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_talking_video)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回视频。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(
            tg_user or msg.from_user,
            api_key,
            f"说话视频（{seconds}s, max={TALKING_VIDEO_MAX_SIDE}, fps={TALKING_VIDEO_FPS}）：{prompt[:40]}",
            cost_time,
        )

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            raw_bytes = await download_url(session, url)
            decoded_media = await asyncio.to_thread(decode_duck_media, raw_bytes)
            if decoded_media:
                send_bytes, result_ext = decoded_media
            else:
                send_bytes = raw_bytes
                result_ext = None
            media_ext = normalize_video_outfit_extension(url, result_ext)
            filename = build_talking_video_filename(url, i, len(result_urls), result_ext)
            caption = (
                f"✅ 说话视频完成！总耗时：{cost_time}s\n"
                f"时长：{seconds}s\n"
                f"规格：最长边 {TALKING_VIDEO_MAX_SIDE}px，{TALKING_VIDEO_FPS}fps\n"
                f"⏳ 这个文件将在凌晨自动清理"
            )
            result_message = await reply_media_with_document_for_images(
                msg,
                media=send_bytes,
                filename=filename,
                caption=caption,
                extension=media_ext,
            )
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

    except TimeoutError as e:
        logger.error("说话视频超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("说话视频处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


async def process_voice_clone(
    msg,
    api_key: str,
    sample_bytes: bytes,
    text: str,
    voice_name: str,
    tg_user=None,
    delete_on_success: Optional[list] = None,
    sample_filename: str = "voice_sample.ogg",
    sample_content_type: str = DEFAULT_AUDIO_CONTENT_TYPE,
):
    if not api_key:
        await reply_autodelete(msg, "⚠️ 还没设置 API Key，请先用 /key 补上。")
        return

    config_error = get_voice_clone_workflow_config_error()
    if config_error:
        await reply_autodelete(msg, f"⚠️ {config_error}")
        return

    status_msg = await reply_text_with_fallback(msg, build_upload_status_message())
    try:
        session = await get_shared_http_session()
        sample_file_name = await rh_upload_audio(
            session,
            api_key,
            sample_bytes,
            sample_filename,
            sample_content_type,
        )
        if not sample_file_name:
            raise ValueError("声音样本上传后未能获取 fileName")

        await safe_edit_text(status_msg, f"⚙️ 声音克隆处理中：{build_prompt_preview(text, 10)}")
        workflow_start = time.time()
        task_id = await rh_run_voice_clone(session, api_key, sample_file_name, text)
        if not task_id:
            raise ValueError("未能获取 taskId")
        logger.info("声音克隆任务已创建，taskId: %s", task_id)

        await safe_edit_text(status_msg, "⏳ 已等待 0s…")

        async def _tick_voice_clone(elapsed: int):
            await safe_edit_text(status_msg, f"⏳ 已等待 {elapsed}s…")

        result_urls, cost_time = await rh_poll_result(session, api_key, task_id, on_tick=_tick_voice_clone)
        if not cost_time:
            cost_time = str(int(time.time() - workflow_start))

        if not result_urls:
            await safe_edit_text(status_msg, "⚠️ 任务完成，但没有返回音频。")
            schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
            return

        await log_usage(tg_user or msg.from_user, api_key, f"声音克隆「{voice_name}」：{text[:40]}", cost_time)

        await safe_edit_text(status_msg, "📤 回传中…")
        result_urls = dedupe_preserving_order(result_urls)
        for i, url in enumerate(result_urls):
            audio_bytes = await download_url(session, url)
            detected_type = audio_content_type_from_bytes(audio_bytes, audio_content_type_from_path(url.split("?", 1)[0]))
            result_ext = audio_extension_for_save(detected_type, url).lstrip(".")
            filename = build_voice_result_filename(url, i, len(result_urls), result_ext)
            reply_markup = None
            audio_token = uuid.uuid4().hex[:12]
            audio_path = build_talking_video_audio_path(audio_token, result_ext)
            try:
                duration_seconds = await get_talking_video_duration_seconds(
                    audio_bytes,
                    filename,
                    detected_type,
                )
                await asyncio.to_thread(audio_path.write_bytes, audio_bytes)
                register_pending_talking_video_audio(
                    audio_token,
                    msg.from_user.id,
                    msg.chat_id,
                    str(audio_path),
                    filename,
                    detected_type,
                    duration_seconds,
                )
                reply_markup = build_talking_video_audio_keyboard(audio_token)
            except Exception:
                logger.exception("缓存说话视频音频失败：token=%s", audio_token)
                cleanup_pending_talking_video_audio(audio_token, {"path": str(audio_path)})
            caption = (
                f"✅ 语音生成完成！总耗时：{cost_time}s\n"
                f"声音角色：{voice_name}\n"
                f"⏳ 这个文件将在凌晨自动清理"
            )
            result_message = await reply_audio_with_fallback(
                msg,
                audio=audio_bytes,
                filename=filename,
                caption=caption,
                reply_markup=reply_markup,
            )
            await register_for_cleanup(result_message.chat_id, result_message.message_id)

        await safe_delete_message(status_msg)

        if delete_on_success:
            for mid in delete_on_success:
                with contextlib.suppress(Exception):
                    await msg.get_bot().delete_message(msg.chat_id, mid)

    except TimeoutError as e:
        logger.error("声音克隆超时: %s", e)
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"⌛ 超时：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))
    except Exception as e:
        logger.exception("声音克隆处理出错")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ 出错了：{e}")
        schedule_background_task(delete_message_later(status_msg.get_bot(), status_msg.chat_id, status_msg.message_id, WARNING_DELETE_SECONDS))


# ─── 命令 Handler ────────────────────────────

def build_help_center(user: dict) -> str:
    api_key = user.get("api_key")
    prompts = user.get("prompts", {})
    image_presets = user.get("image_presets", {})
    voice_presets = user.get("voice_presets", {})
    animation_seconds = normalize_animation_seconds(user)
    prompt_count = len(prompts)
    image_preset_count = len(image_presets)
    voice_preset_count = len(voice_presets)
    key_status = f"已设置（{mask_api_key(api_key)}）" if api_key else "未设置"

    return (
        "👋 发图片即可开始处理\n\n"
        "⚙️ 命令：\n"
        "/key — 设置 API Key\n"
        "/aiprompt — AI 随机风格\n"
        "/save <名称> <内容> — 保存预设\n"
        "/saveimg <名称> — 保存图预设\n"
        "/savevoice <名称> — 保存声音角色\n"
        "/voice — 选择声音角色生成语音\n"
        "/del <名称> — 删除预设\n"
        "/presetflow — 换衣工作流\n"
        "/expand — 扩图方向\n"
        f"/gifsec — 动图{animation_seconds}s\n"
        "/flprompt — 首尾视频默认提示词\n"
        "/talkprompt — 说话视频提示词\n"
        "/comparetext — 设置对比图文案\n"
        "/compareswitch — 对比图功能开关\n"
        "/reset — 清空所有设置\n\n"
        f"📊 文字预设 {prompt_count} 条 · 图片预设 {image_preset_count} 条 · 声音角色 {voice_preset_count} 个\n"
        f"🔑 Key：{key_status}"
    )


def build_bot_commands() -> list[BotCommand]:
    return [
        BotCommand("start", "查看使用菜单"),
        BotCommand("key", "设置 RunningHub API Key"),
        BotCommand("save", "保存文字预设"),
        BotCommand("saveimg", "保存图片预设"),
        BotCommand("savevoice", "保存声音角色"),
        BotCommand("voice", "选择声音角色生成语音"),
        BotCommand("del", "删除预设"),
        BotCommand("aiprompt", "设置 AI 随机风格"),
        BotCommand("gifsec", "切换动图时长"),
        BotCommand("flprompt", "设置首尾视频提示词"),
        BotCommand("talkprompt", "设置说话视频提示词"),
        BotCommand("presetflow", "切换换衣工作流"),
        BotCommand("expand", "设置扩图方向"),
        BotCommand("comparetext", "设置对比图文案"),
        BotCommand("compareswitch", "设置对比图开关"),
        BotCommand("reset", "清空设置"),
    ]


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_start")
    user_id = update.effective_user.id
    # 清理所有等待状态，让用户从卡住的状态脱困
    async with get_user_state_lock(user_id):
        user_states.pop(user_id, None)
    # 清理该用户所有待处理 session
    for sid in list(pending_prompt_sessions.keys()):
        session = pending_prompt_sessions.get(sid)
        if session and session.get("user_id") == user_id:
            expired = pending_prompt_sessions.pop(sid, None)
            cleanup_temp_file(expired.get("image_path") if expired else None)
    for sid in list(pending_video_outfit_sessions.keys()):
        session = pending_video_outfit_sessions.get(sid)
        if session and session.get("user_id") == user_id:
            expired = pending_video_outfit_sessions.pop(sid, None)
            cleanup_temp_file(expired.get("video_path") if expired else None)
    for save_id, info in list(pending_preset_saves.items()):
        if info.get("user_id") == user_id:
            cleanup_pending_preset_file(pending_preset_saves.pop(save_id, None))
    # 只清当前用户的 DS 暂存
    for ds_sid in list(ds_pending.keys()):
        entry = ds_pending.get(ds_sid)
        if entry and entry.get("user_id") == user_id:
            ds_pending.pop(ds_sid, None)
    data = await load_data()
    user = get_user(data, user_id)
    await update.message.reply_text(build_help_center(user))



async def cmd_saveprompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_saveprompt")
    user_id = update.effective_user.id
    if not context.args or len(context.args) < 2:
        await reply_autodelete(update.message, "用法：/save <名称> <提示词内容>", also_delete=update.message)
        return

    name    = context.args[0].strip()
    content = " ".join(context.args[1:]).strip()
    error   = validate_prompt_text(content)
    if error:
        await reply_autodelete(update.message, f"❌ 不能保存：{error}")
        return

    def _mutate(data: dict):
        user = get_user(data, user_id)
        user.setdefault("prompts", {})[name] = content

    await update_data(_mutate)
    await reply_autodelete(update.message, f"✅ 提示词「{name}」已保存。", also_delete=update.message)


async def cmd_saveimg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_saveimg")
    user_id = update.effective_user.id
    preset_name = " ".join(context.args).strip()
    if not preset_name:
        await reply_autodelete(
            update.message,
            "用法：/saveimg <名称>，然后发送图片。\n也可发图时 caption 写 /saveimg <名称>。",
            also_delete=update.message,
        )
        return

    flow_id = uuid.uuid4().hex
    conflict_state = None
    async with get_user_state_lock(user_id):
        current_state = user_states.get(user_id)
        if current_state and current_state.get("state") in WAITING_STATE_LABELS:
            conflict_state = current_state
        else:
            user_states[user_id] = {
                "state": "WAITING_IMAGE_PRESET_IMAGE",
                "preset_name": preset_name,
                "flow_id": flow_id,
                "saveimg_cmd_msg_id": update.message.message_id,
            }
    if conflict_state:
        await reply_autodelete(
            update.message,
            build_waiting_state_conflict_message(conflict_state),
            also_delete=update.message,
        )
        return

    request_msg = await update.message.reply_text(f"🖼️ 请发送要保存为「{preset_name}」的图片：")
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("flow_id") == flow_id:
            state["saveimg_request_msg_id"] = request_msg.message_id

    schedule_background_task(expire_waiting_flow_later(
        user_id,
        "WAITING_IMAGE_PRESET_IMAGE",
        "flow_id",
        flow_id,
        update.message.get_bot(),
        update.message.chat_id,
        request_msg.message_id,
        update.message.message_id,
        flow_id=flow_id,
    ))


async def cmd_savevoice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_savevoice")
    user_id = update.effective_user.id
    preset_name = " ".join(context.args).strip()
    if not preset_name:
        await reply_autodelete(
            update.message,
            "用法：/savevoice <名称>，然后发送一段语音、音频或带声音的视频样本。",
            also_delete=update.message,
        )
        return

    flow_id = uuid.uuid4().hex
    conflict_state = None
    async with get_user_state_lock(user_id):
        current_state = user_states.get(user_id)
        if current_state and current_state.get("state") in WAITING_STATE_LABELS:
            conflict_state = current_state
        else:
            user_states[user_id] = {
                "state": "WAITING_VOICE_SAMPLE",
                "preset_name": preset_name,
                "flow_id": flow_id,
                "savevoice_cmd_msg_id": update.message.message_id,
            }
    if conflict_state:
        await reply_autodelete(
            update.message,
            build_waiting_state_conflict_message(conflict_state),
            also_delete=update.message,
        )
        return

    request_msg = await update.message.reply_text(
        f"🎙️ 请发送要保存为「{preset_name}」的声音样本。\n"
        "可以直接发语音、音频或带声音的视频。建议 10-30 秒、背景干净；请确保你有权使用这个声音。"
    )
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("flow_id") == flow_id:
            state["voice_request_msg_id"] = request_msg.message_id

    schedule_background_task(expire_waiting_flow_later(
        user_id,
        "WAITING_VOICE_SAMPLE",
        "flow_id",
        flow_id,
        update.message.get_bot(),
        update.message.chat_id,
        request_msg.message_id,
        update.message.message_id,
        flow_id=flow_id,
    ))


async def cmd_voice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_voice")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)
    voice_preset_items = list(user.get("voice_presets", {}).items())
    if not voice_preset_items:
        await reply_autodelete(
            update.message,
            "还没有声音角色。先用 /savevoice <名称> 保存一段样本。",
            also_delete=update.message,
        )
        return

    if get_voice_clone_workflow_config_error():
        await update.message.reply_text(
            "🎙️ 已保存声音角色，但声音克隆工作流还没配置。\n"
            "需要配置 RH_VOICE_CLONE_ENDPOINT 和对应节点 ID 后才能生成语音。"
        )

    reply_msg = await update.message.reply_text(
        "🎙️ 请选择声音角色：",
        reply_markup=build_voice_keyboard(voice_preset_items),
    )
    bot = update.message.get_bot()
    chat_id = update.message.chat_id
    panel_key = build_voice_panel_key(chat_id, reply_msg.message_id)
    voice_panel_pending[panel_key] = {
        "cmd_msg_id": update.message.message_id,
        "chat_id": chat_id,
        "user_id": user_id,
    }
    schedule_background_task(expire_voice_panel_later(
        panel_key,
        bot,
        chat_id,
        reply_msg.message_id,
        update.message.message_id,
    ))


async def cmd_delprompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_delprompt")
    user_id = update.effective_user.id
    if not context.args:
        await reply_autodelete(update.message, "用法：/del <名称>", also_delete=update.message)
        return

    name = context.args[0].strip()
    def _mutate(data: dict):
        user = get_user(data, user_id)

        # 优先匹配文字预设
        if name in user.get("prompts", {}):
            del user["prompts"][name]
            return "text", None

        # 再匹配图片预设
        if name in user.get("image_presets", {}):
            return "image", user["image_presets"].pop(name)

        # 最后匹配声音角色
        if name in user.get("voice_presets", {}):
            return "voice", voice_preset_path(user["voice_presets"].pop(name))

        return None, None

    deleted_type, deleted_path = await update_data(_mutate)
    if deleted_type == "text":
        await reply_autodelete(update.message, f"🗑️ 预设「{name}」已删除。", also_delete=update.message)
        return

    if deleted_type == "image":
        await cleanup_image_preset_file_if_unreferenced(deleted_path)
        await reply_autodelete(update.message, f"🗑️ 图片预设「{name}」已删除。", also_delete=update.message)
        return

    if deleted_type == "voice":
        await cleanup_voice_preset_file_if_unreferenced(deleted_path)
        await reply_autodelete(update.message, f"🗑️ 声音角色「{name}」已删除。", also_delete=update.message)
        return

    await reply_autodelete(update.message, f"❌ 没有找到预设「{name}」。", also_delete=update.message)




# ─── 图片接收与键盘展示 ───────────────────────

def build_prompt_keyboard(session_id: str, prompt_items: list[tuple[str, str]], image_preset_items: list[tuple[str, str]] = None) -> InlineKeyboardMarkup:
    buttons = []
    for idx, (name, _content) in enumerate(prompt_items[:MAX_PROMPTS_SHOWN]):
        buttons.append([InlineKeyboardButton(name, callback_data=f"prompt:{session_id}:{idx}")])
    if image_preset_items:
        for idx, (name, _path) in enumerate(image_preset_items):
            buttons.append([InlineKeyboardButton(name, callback_data=f"prompt:{session_id}:img:{idx}")])
    buttons.append([
        InlineKeyboardButton("参考换衣", callback_data=f"prompt:{session_id}:refoutfit"),
        InlineKeyboardButton("参考换脸", callback_data=f"prompt:{session_id}:faceswap"),
        InlineKeyboardButton("场景换人", callback_data=f"prompt:{session_id}:scene_replace"),
    ])
    buttons.append([
        InlineKeyboardButton("首尾视频", callback_data=f"prompt:{session_id}:firstlast"),
        InlineKeyboardButton("生成动图", callback_data=f"prompt:{session_id}:gif"),
        InlineKeyboardButton("图片扩展", callback_data=f"prompt:{session_id}:extend"),
    ])
    buttons.append([
        InlineKeyboardButton("说话视频", callback_data=f"prompt:{session_id}:talking_video"),
        InlineKeyboardButton("提取装扮",  callback_data=f"prompt:{session_id}:outfit"),
        InlineKeyboardButton("随机预设", callback_data=f"prompt:{session_id}:dsgen"),
    ])
    buttons.append([InlineKeyboardButton("自定输入", callback_data=f"prompt:{session_id}:custom")])
    return InlineKeyboardMarkup(buttons)


def build_animation_prompt_keyboard(session_id: str, prompt_items: list[tuple[str, str]]) -> Optional[InlineKeyboardMarkup]:
    buttons = [
        [InlineKeyboardButton(name, callback_data=f"animation:{session_id}:{idx}")]
        for idx, (name, _content) in enumerate(prompt_items[:MAX_PROMPTS_SHOWN])
    ]
    return InlineKeyboardMarkup(buttons) if buttons else None


def build_first_last_prompt_keyboard(session_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏭️ 跳过（用默认提示词）", callback_data=f"firstlastprompt:{session_id}:skip"),
    ]])


def build_video_outfit_keyboard(session_id: str, image_preset_items: list[tuple[str, str]] = None) -> InlineKeyboardMarkup:
    buttons = []
    for idx, (name, _path) in enumerate(image_preset_items or []):
        buttons.append([InlineKeyboardButton(name, callback_data=f"videooutfit:{session_id}:img:{idx}")])
    buttons.append([InlineKeyboardButton("📤 上传参考图", callback_data=f"videooutfit:{session_id}:upload")])
    return InlineKeyboardMarkup(buttons)


def build_video_outfit_source_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬇️ 下载源文件", callback_data=f"videooutfitdl:{token}")]
    ])


def build_talking_video_audio_keyboard(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("说话视频", callback_data=f"talkvid:{token}")]
    ])


def build_voice_panel_key(chat_id: int, panel_msg_id: int) -> str:
    return f"{chat_id}:{panel_msg_id}"


async def expire_voice_panel_later(
    panel_key: str,
    bot,
    chat_id: int,
    panel_msg_id: int,
    cmd_msg_id: int,
    delay_seconds: int = WAITING_FLOW_TIMEOUT_SECONDS,
):
    """5分钟未选择声音角色时，删除面板并清理内存记录。"""
    await asyncio.sleep(delay_seconds)
    pending = voice_panel_pending.pop(panel_key, None)
    if not pending:
        return
    for mid in filter(None, [panel_msg_id, cmd_msg_id]):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, mid)


async def cleanup_voice_panel_messages(query) -> tuple:
    """清理 /voice 命令消息和 bot 的角色按钮面板。"""
    bot = query.get_bot()
    chat_id = query.message.chat_id
    panel_msg_id = query.message.message_id
    pending = voice_panel_pending.pop(build_voice_panel_key(chat_id, panel_msg_id), None) or {}
    cmd_msg_id = pending.get("cmd_msg_id")
    if cmd_msg_id is None:
        cmd_msg_id = getattr(getattr(query.message, "reply_to_message", None), "message_id", None)
    for mid in filter(None, [panel_msg_id, cmd_msg_id]):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, mid)
    return bot, chat_id, panel_msg_id


def build_voice_keyboard(voice_preset_items: list[tuple[str, dict]]) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(name, callback_data=f"voice:{idx}")]
        for idx, (name, _info) in enumerate(voice_preset_items)
    ]
    buttons.append([InlineKeyboardButton("取消", callback_data="voice:cancel")])
    return InlineKeyboardMarkup(buttons)


def build_ds_keyboard(ds_sid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ 确定", callback_data=f"ds:{ds_sid}:confirm"),
        InlineKeyboardButton("🔄 重试", callback_data=f"ds:{ds_sid}:retry"),
        InlineKeyboardButton("❌ 取消", callback_data=f"ds:{ds_sid}:cancel"),
    ]])


def build_ds_prompt_message(prompt: str) -> str:
    return f"🤖 预设内容：{prompt}"


async def edit_or_reply_ds_prompt_message(message, ds_sid: str, prompt: str, *, log_context: str):
    text = build_ds_prompt_message(prompt)
    reply_markup = build_ds_keyboard(ds_sid)
    if await safe_edit_text(message, text, log_context=log_context, reply_markup=reply_markup):
        return message

    sent = await reply_text_with_fallback(message, text, reply_markup=reply_markup)
    with contextlib.suppress(Exception):
        await message.delete()
    return sent


async def create_prompt_session(
    user_id: int,
    bot,
    chat_id: int,
    source_message_id: int,
    image_bytes: bytes,
    image_path: Optional[str],
    image_filename: str,
    image_content_type: str,
    prompt_items: list[tuple[str, str]],
    image_preset_items: list[tuple[str, str]] = None,
) -> str:
    session_id = uuid.uuid4().hex[:12]
    if image_path is None:
        save_path = build_session_image_path(session_id, image_content_type, image_filename)
        await asyncio.to_thread(save_path.write_bytes, image_bytes)
        image_path = str(save_path)
    pending_prompt_sessions[session_id] = {
        "user_id": user_id,
        "bot": bot,
        "chat_id": chat_id,
        "source_message_id": source_message_id,
        "image_bytes": None,
        "image_path": image_path,
        "image_filename": image_filename,
        "image_content_type": image_content_type,
        "prompt_items": prompt_items,
        "image_preset_items": image_preset_items or [],
    }
    schedule_background_task(expire_prompt_session_later(session_id))
    return session_id


async def create_video_outfit_session(
    user_id: int,
    bot,
    chat_id: int,
    source_message_id: int,
    video_bytes: bytes,
    video_filename: str,
    video_content_type: str,
    image_preset_items: list[tuple[str, str]] = None,
) -> str:
    session_id = uuid.uuid4().hex[:12]
    save_path = build_session_video_path(session_id, video_content_type, video_filename)
    await asyncio.to_thread(save_path.write_bytes, video_bytes)
    pending_video_outfit_sessions[session_id] = {
        "user_id": user_id,
        "bot": bot,
        "chat_id": chat_id,
        "source_message_id": source_message_id,
        "video_bytes": None,
        "video_path": str(save_path),
        "video_filename": video_filename,
        "video_content_type": video_content_type,
        "image_preset_items": image_preset_items or [],
    }
    schedule_background_task(expire_video_outfit_session_later(session_id))
    return session_id



async def _receive_image(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    image_bytes: bytes,
    image_path: Optional[str],
    image_filename: str,
    image_content_type: str,
):
    """收到图片后的统一处理：有 caption 直接处理，否则展示键盘。"""
    msg     = update.message
    user_id = msg.from_user.id

    data    = await load_data()
    user    = get_user(data, user_id)
    api_key = user.get("api_key")
    if not api_key:
        cleanup_temp_file(image_path)
        await reply_autodelete(
            msg,
            "⚠️ 请先设置 API Key：\n/key <你的 RunningHub API Key>"
        )
        return

    # 有 key 但没提示词时，也可以直接继续给自定义入口
    prompt_items = list(user.get("prompts", {}).items())
    image_preset_items = list(user.get("image_presets", {}).items())  # [(name, path), ...]

    # 有 caption 直接使用
    if msg.caption and msg.caption.strip():
        prompt = msg.caption.strip()
        error = validate_prompt_text(prompt)
        if error:
            warning_message = await msg.reply_text(build_prompt_validation_reply(error))
            schedule_background_task(delete_message_later(msg.get_bot(), msg.chat_id, msg.message_id, WARNING_DELETE_SECONDS))
            schedule_background_task(delete_message_later(warning_message.get_bot(), warning_message.chat_id, warning_message.message_id, WARNING_DELETE_SECONDS))
            cleanup_temp_file(image_path)
            return
        await register_for_cleanup(msg.chat_id, msg.message_id)
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_image(
                msg,
                api_key,
                image_bytes,
                prompt,
                image_filename=image_filename,
                image_content_type=image_content_type,
            ),
        )
        if not scheduled:
            cleanup_temp_file(image_path)
            await reply_autodelete(msg, build_user_task_limit_message(count))
        return

    # 存图片，展示选择键盘
    session_id = await create_prompt_session(
        user_id,
        context.bot,
        msg.chat_id,
        msg.message_id,
        image_bytes,
        image_path,
        image_filename,
        image_content_type,
        prompt_items,
        image_preset_items,
    )
    keyboard = build_prompt_keyboard(session_id, prompt_items, image_preset_items)
    keyboard_msg = await msg.reply_text("📷 收到图片！请选择提示词：", reply_markup=keyboard)
    await register_for_cleanup(msg.chat_id, msg.message_id, keyboard_msg.message_id)


async def _receive_video(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    video_bytes: bytes,
    video_filename: str,
    video_content_type: str,
):
    """收到视频后展示视频换衣参考图选择面板。"""
    msg = update.message
    user_id = msg.from_user.id

    state = user_states.get(user_id)
    if state and state.get("state") in WAITING_STATE_LABELS:
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
        return

    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")
    if not api_key:
        await reply_autodelete(
            msg,
            "⚠️ 请先设置 API Key：\n/key <你的 RunningHub API Key>"
        )
        return

    image_preset_items = list(user.get("image_presets", {}).items())
    session_id = await create_video_outfit_session(
        user_id,
        context.bot,
        msg.chat_id,
        msg.message_id,
        video_bytes,
        video_filename,
        video_content_type,
        image_preset_items,
    )
    keyboard_msg = await msg.reply_text(
        "🎬 收到视频，请选择衣服参考：",
        reply_markup=build_video_outfit_keyboard(session_id, image_preset_items),
    )
    pending_video_outfit_sessions[session_id]["panel_message_id"] = keyboard_msg.message_id
    await register_for_cleanup(msg.chat_id, msg.message_id, keyboard_msg.message_id)


async def _receive_face_image(
    update: Update,
    face_bytes: bytes,
    face_filename: str,
    face_content_type: str,
):
    """收到脸部参考图后，直接触发换脸工作流。"""
    msg     = update.message
    user_id = msg.from_user.id
    state   = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session    = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 原图已过期，请重新发图。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    state = await pop_user_state_if_same(user_id, "WAITING_FACE_IMAGE", session_id=session_id)
    if not state:
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(msg, "⚠️ 会话已过期，请重新上传图片。")
        return
    api_key_data = await load_data()
    api_key      = get_user(api_key_data, user_id).get("api_key")

    face_request_msg_id = session.pop("face_request_msg_id", None)
    for mid in filter(None, [msg.message_id, face_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_faceswap(
            msg,
            api_key,
            image_bytes,
            face_bytes,
            image_filename=session.get("image_filename", "original.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            face_filename=face_filename,
            face_content_type=face_content_type,
        ),
    )
    if not scheduled:
        await reply_autodelete(msg, build_user_task_limit_message(count))


async def _receive_last_frame_image(
    update: Update,
    last_bytes: bytes,
    last_filename: str,
    last_content_type: str,
):
    """收到尾帧图片后，引导用户输入提示词或跳过用默认。"""
    msg = update.message
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 首帧已过期，请重新发图。")
        return

    # 先把 WAITING_LAST_FRAME_IMAGE 释放掉，再切换到 WAITING_FIRST_LAST_PROMPT
    popped = await pop_user_state_if_same(user_id, "WAITING_LAST_FRAME_IMAGE", session_id=session_id)
    if not popped:
        return
    flow_id = uuid.uuid4().hex
    success, current_state, already_same = await claim_user_state(
        user_id,
        {"state": "WAITING_FIRST_LAST_PROMPT", "session_id": session_id, "flow_id": flow_id},
        "WAITING_FIRST_LAST_PROMPT",
        session_id=session_id,
    )
    if not success:
        await reply_autodelete(msg, build_waiting_state_conflict_message(current_state))
        return

    # 保存尾帧到 session，等待提示词
    session["last_frame_bytes"] = last_bytes
    session["last_frame_filename"] = last_filename
    session["last_frame_content_type"] = last_content_type

    last_frame_request_msg_id = session.pop("last_frame_request_msg_id", None)
    for mid in filter(None, [msg.message_id, last_frame_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    prompt_msg = await msg.reply_text(
        "✏️ 请输入首尾视频提示词，或点击下方按钮使用默认提示词：",
        reply_markup=build_first_last_prompt_keyboard(session_id),
    )
    session["first_last_prompt_request_msg_id"] = prompt_msg.message_id
    await register_for_cleanup(msg.chat_id, prompt_msg.message_id)
    schedule_background_task(expire_waiting_flow_later(
        user_id,
        "WAITING_FIRST_LAST_PROMPT",
        "session_id",
        session_id,
        msg.get_bot(),
        msg.chat_id,
        prompt_msg.message_id,
        flow_id=flow_id,
    ))


async def _start_first_last_video(
    target_msg,
    user_id: int,
    session_id: str,
    session: dict,
    prompt: str,
    tg_user,
):
    """实际触发首尾视频工作流：从 session 取出首帧+尾帧，调度任务。"""
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(target_msg, build_user_task_limit_message())
        return False

    first_bytes = await get_session_image_bytes(session)
    if not first_bytes:
        await reply_autodelete(target_msg, "⚠️ 首帧已过期，请重新发图。")
        return False
    last_bytes = session.get("last_frame_bytes")
    if not last_bytes:
        await reply_autodelete(target_msg, "⚠️ 尾帧已过期，请重新点击「首尾视频」。")
        return False

    api_key_data = await load_data()
    user = get_user(api_key_data, user_id)
    api_key = user.get("api_key")
    seconds = normalize_animation_seconds(user)

    last_filename = session.get("last_frame_filename", "last.jpg")
    last_content_type = session.get("last_frame_content_type", DEFAULT_IMAGE_CONTENT_TYPE)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_first_last_video(
            target_msg,
            api_key,
            first_bytes,
            last_bytes,
            seconds=seconds,
            prompt=prompt,
            tg_user=tg_user,
            first_filename=session.get("image_filename", "first.jpg"),
            first_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            last_filename=last_filename,
            last_content_type=last_content_type,
        ),
    )
    if not scheduled:
        await reply_autodelete(target_msg, build_user_task_limit_message(count))
        return False

    # 释放尾帧字节，避免长期占内存
    session.pop("last_frame_bytes", None)
    return True


async def _receive_talking_video_image(
    update: Update,
    image_bytes: bytes,
    image_filename: str,
    image_content_type: str,
):
    """生成音频结果下点击「说话视频」后，收到图片并触发工作流。"""
    msg = update.message
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state:
        return
    save_id = state.get("save_id")
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    current_state = await pop_user_state_if_same(user_id, "WAITING_TALKING_VIDEO_IMAGE", save_id=save_id)
    if not current_state:
        return

    audio_path = current_state.get("audio_path")
    if not audio_path:
        await reply_autodelete(msg, "⚠️ 音频已过期，请重新生成语音。")
        return
    try:
        audio_bytes = await asyncio.to_thread(Path(audio_path).read_bytes)
    except Exception:
        await reply_autodelete(msg, "⚠️ 音频读取失败，请重新生成语音。")
        return

    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")
    prompt = get_user_talking_video_prompt(user)
    seconds = normalize_talking_video_seconds(current_state.get("duration_seconds"))
    audio_filename = current_state.get("audio_filename") or Path(audio_path).name
    audio_content_type = current_state.get("audio_content_type") or audio_content_type_from_path(audio_filename)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_talking_video(
            msg,
            api_key,
            image_bytes,
            audio_bytes,
            seconds,
            prompt,
            tg_user=msg.from_user,
            image_filename=image_filename,
            image_content_type=image_content_type,
            audio_filename=audio_filename_for_upload(audio_filename, "speech", audio_content_type),
            audio_content_type=audio_content_type,
        ),
    )
    if not scheduled:
        async with get_user_state_lock(user_id):
            if user_id not in user_states:
                user_states[user_id] = current_state
        await reply_autodelete(msg, build_user_task_limit_message(count))
        return

    for mid in filter(None, [msg.message_id, current_state.get("talking_video_image_request_msg_id")]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)


async def _receive_talking_video_audio(
    update: Update,
    audio_bytes: bytes,
    audio_filename: str,
    audio_content_type: str,
    hinted_duration: Optional[float] = None,
):
    """图片面板点击「说话视频」后，收到音频并触发工作流。"""
    msg = update.message
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 图片已过期，请重新发图。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    current_state = await pop_user_state_if_same(user_id, "WAITING_TALKING_VIDEO_AUDIO", session_id=session_id)
    if not current_state:
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(msg, "⚠️ 会话已过期，请重新上传图片。")
        return

    seconds = await get_talking_video_duration_seconds(
        audio_bytes,
        audio_filename,
        audio_content_type,
        hinted_duration,
    )
    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")
    prompt = get_user_talking_video_prompt(user)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_talking_video(
            msg,
            api_key,
            image_bytes,
            audio_bytes,
            seconds,
            prompt,
            tg_user=msg.from_user,
            image_filename=session.get("image_filename", "portrait.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            audio_filename=audio_filename_for_upload(audio_filename, "speech", audio_content_type),
            audio_content_type=audio_content_type,
        ),
    )
    if not scheduled:
        async with get_user_state_lock(user_id):
            if user_id not in user_states:
                user_states[user_id] = current_state
        await reply_autodelete(msg, build_user_task_limit_message(count))
        return

    talking_video_audio_request_msg_id = session.pop("talking_video_audio_request_msg_id", None)
    for mid in filter(None, [msg.message_id, talking_video_audio_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)


async def _receive_talking_video_audio_from_video(
    update: Update,
    video_bytes: bytes,
    video_filename: str,
    video_content_type: str,
    hinted_duration: Optional[float] = None,
):
    msg = update.message
    status_msg = await reply_text_with_fallback(msg, "🎧 正在从视频提取音轨…")
    try:
        audio_bytes, audio_filename, audio_content_type = await extract_audio_from_video_bytes(
            video_bytes,
            video_filename,
            video_content_type,
        )
    except Exception as e:
        logger.exception("从视频提取说话视频音频失败")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ {e}")
        schedule_background_task(delete_message_later(
            status_msg.get_bot(),
            status_msg.chat_id,
            status_msg.message_id,
            WARNING_DELETE_SECONDS,
        ))
        return

    await safe_delete_message(status_msg)
    await _receive_talking_video_audio(update, audio_bytes, audio_filename, audio_content_type, hinted_duration)


async def _save_image_preset_from_caption(
    update: Update,
    image_bytes: bytes,
    image_filename: str,
    image_content_type: str,
) -> bool:
    """如果 caption 是 /saveimg <名称>，保存图片预设并返回 True，否则返回 False。"""
    msg     = update.message
    preset_name = parse_saveimg_name(msg.caption)
    if preset_name is None:
        return False
    if not preset_name:
        await msg.reply_text("用法：发图时 caption 写 /saveimg <名称>")
        return True

    user_id = msg.from_user.id
    state_cleanup_ids = []
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("state") == "WAITING_IMAGE_PRESET_IMAGE":
            current_state = user_states.pop(user_id, None) or {}
            state_cleanup_ids = [
                current_state.get("saveimg_request_msg_id"),
                current_state.get("saveimg_cmd_msg_id"),
            ]

    await save_image_preset_file(
        user_id,
        preset_name,
        image_bytes,
        image_filename,
        image_content_type,
    )

    # 删掉用户发的图（保持聊天清洁），然后提示成功
    for mid in filter(None, state_cleanup_ids):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)
    with contextlib.suppress(Exception):
        await msg.delete()
    await send_autodelete_message(msg.get_bot(), msg.chat_id, f"✅ 图片预设「{preset_name}」已保存。")
    return True


async def _receive_image_preset_image(
    update: Update,
    image_bytes: bytes,
    image_filename: str,
    image_content_type: str,
):
    msg = update.message
    user_id = msg.from_user.id
    current_state = await pop_user_state_if_same(user_id, "WAITING_IMAGE_PRESET_IMAGE")
    if not current_state:
        return

    preset_name = (current_state.get("preset_name") or "").strip()
    if not preset_name:
        await reply_autodelete(msg, "⚠️ 预设名称已失效，请重新使用 /saveimg <名称>。")
        return

    await save_image_preset_file(
        user_id,
        preset_name,
        image_bytes,
        image_filename,
        image_content_type,
    )

    bot = msg.get_bot()
    chat_id = msg.chat_id
    for mid in filter(None, [
        msg.message_id,
        current_state.get("saveimg_request_msg_id"),
        current_state.get("saveimg_cmd_msg_id"),
    ]):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, mid)

    await send_autodelete_message(bot, chat_id, f"✅ 图片预设「{preset_name}」已保存。")


async def _receive_voice_sample(
    update: Update,
    audio_bytes: bytes,
    audio_filename: str,
    audio_content_type: str,
):
    msg = update.message
    user_id = msg.from_user.id
    current_state = await pop_user_state_if_same(user_id, "WAITING_VOICE_SAMPLE")
    if not current_state:
        return

    preset_name = (current_state.get("preset_name") or "").strip()
    if not preset_name:
        await reply_autodelete(msg, "⚠️ 声音角色名称已失效，请重新使用 /savevoice <名称>。")
        return

    await save_voice_preset_file(
        user_id,
        preset_name,
        audio_bytes,
        audio_filename,
        audio_content_type,
    )

    bot = msg.get_bot()
    chat_id = msg.chat_id
    for mid in filter(None, [
        msg.message_id,
        current_state.get("voice_request_msg_id"),
        current_state.get("savevoice_cmd_msg_id"),
    ]):
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, mid)

    await send_autodelete_message(bot, chat_id, f"✅ 声音角色「{preset_name}」已保存。")


async def _receive_voice_sample_from_video(
    update: Update,
    video_bytes: bytes,
    video_filename: str,
    video_content_type: str,
):
    msg = update.message
    status_msg = await reply_text_with_fallback(msg, "🎧 正在从视频提取音轨…")
    try:
        audio_bytes, audio_filename, audio_content_type = await extract_audio_from_video_bytes(
            video_bytes,
            video_filename,
            video_content_type,
        )
    except Exception as e:
        logger.exception("从视频提取声音样本失败")
        with contextlib.suppress(Exception):
            await safe_edit_text(status_msg, f"❌ {e}")
        schedule_background_task(delete_message_later(
            status_msg.get_bot(),
            status_msg.chat_id,
            status_msg.message_id,
            WARNING_DELETE_SECONDS,
        ))
        return

    await safe_delete_message(status_msg)
    await _receive_voice_sample(update, audio_bytes, audio_filename, audio_content_type)


async def _receive_body_ref_image(
    update: Update,
    ref_bytes: bytes,
    ref_filename: str,
    ref_content_type: str,
):
    """收到参考服装图后，按用户配置触发参考换衣工作流。"""
    msg     = update.message
    user_id = msg.from_user.id
    state   = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session    = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 原图已过期，请重新发图。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    state = await pop_user_state_if_same(user_id, "WAITING_BODY_REF_IMAGE", session_id=session_id)
    if not state:
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(msg, "⚠️ 会话已过期，请重新上传图片。")
        return
    api_key_data = await load_data()
    user_cfg = get_user(api_key_data, user_id)
    api_key = user_cfg.get("api_key")
    preset_workflow = user_cfg.get("preset_workflow", DEFAULT_PRESET_WORKFLOW_KEY)
    process_fn = process_qwen_outfit if preset_workflow == "qwen" else process_bodyswap

    body_request_msg_id = session.pop("body_request_msg_id", None)
    for mid in filter(None, [msg.message_id, body_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_fn(
            msg,
            api_key,
            image_bytes,
            ref_bytes,
            image_filename=session.get("image_filename", "original.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ref_filename=ref_filename,
            ref_content_type=ref_content_type,
        ),
    )
    if not scheduled:
        await reply_autodelete(msg, build_user_task_limit_message(count))


async def _receive_qwen_ref_image(
    update: Update,
    ref_bytes: bytes,
    ref_filename: str,
    ref_content_type: str,
):
    """收到参考服装图后，触发 qwen 换装工作流。"""
    msg     = update.message
    user_id = msg.from_user.id
    state   = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session    = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 原图已过期，请重新发图。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    state = await pop_user_state_if_same(user_id, "WAITING_QWEN_BODY_REF_IMAGE", session_id=session_id)
    if not state:
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(msg, "⚠️ 会话已过期，请重新上传图片。")
        return
    api_key_data = await load_data()
    api_key      = get_user(api_key_data, user_id).get("api_key")

    qwen_request_msg_id = session.pop("qwen_request_msg_id", None)
    for mid in filter(None, [msg.message_id, qwen_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_qwen_outfit(
            msg,
            api_key,
            image_bytes,
            ref_bytes,
            image_filename=session.get("image_filename", "original.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ref_filename=ref_filename,
            ref_content_type=ref_content_type,
        ),
    )
    if not scheduled:
        await reply_autodelete(msg, build_user_task_limit_message(count))


async def _receive_scene_person_image(
    update: Update,
    person_bytes: bytes,
    person_filename: str,
    person_content_type: str,
):
    """收到人物图后，用原图作为场景图触发场景换人工作流。"""
    msg = update.message
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session = pending_prompt_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 场景图已过期，请重新发图。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    state = await pop_user_state_if_same(user_id, "WAITING_SCENE_PERSON_IMAGE", session_id=session_id)
    if not state:
        return

    scene_bytes = await get_session_image_bytes(session)
    if not scene_bytes:
        await reply_autodelete(msg, "⚠️ 会话已过期，请重新上传图片。")
        return
    api_key_data = await load_data()
    api_key = get_user(api_key_data, user_id).get("api_key")

    scene_person_request_msg_id = session.pop("scene_person_request_msg_id", None)
    for mid in filter(None, [msg.message_id, scene_person_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_scene_replace(
            msg,
            api_key,
            scene_bytes,
            person_bytes,
            tg_user=msg.from_user,
            scene_filename=session.get("image_filename", "scene.jpg"),
            scene_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            person_filename=person_filename,
            person_content_type=person_content_type,
        ),
    )
    if not scheduled:
        await reply_autodelete(msg, build_user_task_limit_message(count))


async def _receive_video_outfit_ref_image(
    update: Update,
    ref_bytes: bytes,
    ref_filename: str,
    ref_content_type: str,
):
    """收到视频换衣参考图后，触发视频换衣工作流。"""
    msg = update.message
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state:
        return
    session_id = state.get("session_id")
    session = pending_video_outfit_sessions.get(session_id)
    if not session:
        await reply_autodelete(msg, "⚠️ 视频已过期，请重新发送视频。")
        return
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(msg, build_user_task_limit_message())
        return

    state = await pop_user_state_if_same(user_id, "WAITING_VIDEO_OUTFIT_IMAGE", session_id=session_id)
    if not state:
        return

    video_bytes = await get_video_outfit_session_bytes(session)
    if not video_bytes:
        await reply_autodelete(msg, "⚠️ 视频已过期，请重新发送视频。")
        return
    if session.get("processing"):
        await reply_autodelete(msg, "🎬 视频换衣处理中，请等完成后再试。")
        return
    api_key_data = await load_data()
    api_key = get_user(api_key_data, user_id).get("api_key")

    video_ref_request_msg_id = session.pop("video_ref_request_msg_id", None)
    for mid in filter(None, [msg.message_id, video_ref_request_msg_id]):
        with contextlib.suppress(Exception):
            await msg.get_bot().delete_message(msg.chat_id, mid)

    session["processing"] = True
    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_video_outfit(
            msg,
            api_key,
            video_bytes,
            ref_bytes,
            video_filename=session.get("video_filename", "input.mp4"),
            video_content_type=session.get("video_content_type", DEFAULT_VIDEO_CONTENT_TYPE),
            ref_filename=ref_filename,
            ref_content_type=ref_content_type,
            video_session_id=session_id,
            video_session=session,
        ),
    )
    if not scheduled:
        session["processing"] = False
        await reply_autodelete(msg, build_user_task_limit_message(count))
        return


async def _handle_custom_prompt_media(
    update: Update,
):
    """自定义提示词状态下收到图片时，提示用户改发文字提示词。"""
    msg = update.message
    await reply_autodelete(
        msg,
        "✏️ 当前在等待提示词，请直接发送文字提示词。\n"
        "如果要参考换衣，请点击「参考换衣」。"
    )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_photo")
    msg = update.message
    with contextlib.suppress(Exception):
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_PHOTO)

    user_id = msg.from_user.id
    photo   = msg.photo[-1]
    tg_file = await context.bot.get_file(photo.file_id)
    image_filename = image_filename_for_upload("photo.jpg", "photo.jpg", DEFAULT_IMAGE_CONTENT_TYPE)
    image_content_type = DEFAULT_IMAGE_CONTENT_TYPE
    image_bytes = bytes(await tg_file.download_as_bytearray())
    if await _save_image_preset_from_caption(update, image_bytes, image_filename, image_content_type):
        return
    state = user_states.get(user_id)
    cur_state = state.get("state") if state else None
    if cur_state == "WAITING_PRESET_NAME":
        await reply_text_with_fallback(msg, "💾 请先输入预设名称，保存完成后再继续发图。")
    elif cur_state == "WAITING_IMAGE_PRESET_IMAGE":
        await _receive_image_preset_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_FACE_IMAGE":
        await _receive_face_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_BODY_REF_IMAGE":
        await _receive_body_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_SCENE_PERSON_IMAGE":
        await _receive_scene_person_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_VIDEO_OUTFIT_IMAGE":
        await _receive_video_outfit_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_LAST_FRAME_IMAGE":
        await _receive_last_frame_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_QWEN_BODY_REF_IMAGE":
        await _receive_qwen_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_TALKING_VIDEO_IMAGE":
        await _receive_talking_video_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_TALKING_VIDEO_AUDIO":
        await reply_autodelete(msg, "请发送音频文件、语音，或带声音的视频来生成说话视频。")
    elif cur_state == "WAITING_CUSTOM_PROMPT":
        await _handle_custom_prompt_media(update)
    elif cur_state in ("WAITING_VOICE_SAMPLE", "WAITING_VOICE_TEXT"):
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
    else:
        await _receive_image(update, context, image_bytes, None, image_filename, image_content_type)


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_document")
    msg = update.message
    with contextlib.suppress(Exception):
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_DOCUMENT)

    user_id = msg.from_user.id
    doc = msg.document
    if not doc.mime_type or not doc.mime_type.startswith("image/"):
        await reply_autodelete(msg, "请发送图片文件（jpg/png）")
        return
    image_content_type = normalize_image_content_type(doc.mime_type, doc.file_name)
    image_filename = image_filename_for_upload(doc.file_name, "image", image_content_type)
    tg_file = await context.bot.get_file(doc.file_id)
    image_bytes = bytes(await tg_file.download_as_bytearray())
    if await _save_image_preset_from_caption(update, image_bytes, image_filename, image_content_type):
        return
    state = user_states.get(user_id)
    cur_state = state.get("state") if state else None
    if cur_state == "WAITING_PRESET_NAME":
        await reply_text_with_fallback(update.message, "💾 请先输入预设名称，保存完成后再继续发图。")
    elif cur_state == "WAITING_IMAGE_PRESET_IMAGE":
        await _receive_image_preset_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_FACE_IMAGE":
        await _receive_face_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_BODY_REF_IMAGE":
        await _receive_body_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_SCENE_PERSON_IMAGE":
        await _receive_scene_person_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_VIDEO_OUTFIT_IMAGE":
        await _receive_video_outfit_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_LAST_FRAME_IMAGE":
        await _receive_last_frame_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_QWEN_BODY_REF_IMAGE":
        await _receive_qwen_ref_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_TALKING_VIDEO_IMAGE":
        await _receive_talking_video_image(update, image_bytes, image_filename, image_content_type)
    elif cur_state == "WAITING_TALKING_VIDEO_AUDIO":
        await reply_autodelete(msg, "请发送音频文件、语音，或带声音的视频来生成说话视频。")
    elif cur_state == "WAITING_CUSTOM_PROMPT":
        await _handle_custom_prompt_media(update)
    elif cur_state in ("WAITING_VOICE_SAMPLE", "WAITING_VOICE_TEXT"):
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
    else:
        await _receive_image(update, context, image_bytes, None, image_filename, image_content_type)


async def handle_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_video")
    msg = update.message
    with contextlib.suppress(Exception):
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_VIDEO)

    user_id = msg.from_user.id
    state = user_states.get(user_id)
    cur_state = state.get("state") if state else None
    video = msg.video
    video_content_type = normalize_video_content_type(getattr(video, "mime_type", None), getattr(video, "file_name", None))
    video_filename = video_filename_for_upload(getattr(video, "file_name", None), "video", video_content_type)
    if cur_state == "WAITING_VOICE_SAMPLE":
        tg_file = await context.bot.get_file(video.file_id)
        video_bytes = bytes(await tg_file.download_as_bytearray())
        await _receive_voice_sample_from_video(update, video_bytes, video_filename, video_content_type)
        return
    if cur_state == "WAITING_TALKING_VIDEO_AUDIO":
        tg_file = await context.bot.get_file(video.file_id)
        video_bytes = bytes(await tg_file.download_as_bytearray())
        await _receive_talking_video_audio_from_video(
            update,
            video_bytes,
            video_filename,
            video_content_type,
            getattr(video, "duration", None),
        )
        return
    if cur_state == "WAITING_TALKING_VIDEO_IMAGE":
        await reply_autodelete(msg, "当前在等待图片，请直接发送要说话的人像图片。")
        return
    if cur_state == "WAITING_VOICE_TEXT":
        await reply_autodelete(msg, "当前在等待语音文案，请直接发送文字。")
        return
    if state and state.get("state") in WAITING_STATE_LABELS:
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
        return
    data = await load_data()
    if not get_user(data, user_id).get("api_key"):
        await reply_autodelete(msg, "⚠️ 请先设置 API Key：\n/key <你的 RunningHub API Key>")
        return

    tg_file = await context.bot.get_file(video.file_id)
    video_bytes = bytes(await tg_file.download_as_bytearray())
    await _receive_video(update, context, video_bytes, video_filename, video_content_type)


async def handle_video_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_video_document")
    msg = update.message
    with contextlib.suppress(Exception):
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_VIDEO)

    user_id = msg.from_user.id
    state = user_states.get(user_id)
    cur_state = state.get("state") if state else None
    doc = msg.document
    video_content_type = normalize_video_content_type(doc.mime_type, doc.file_name)
    video_filename = video_filename_for_upload(doc.file_name, "video", video_content_type)
    if cur_state == "WAITING_VOICE_SAMPLE":
        tg_file = await context.bot.get_file(doc.file_id)
        video_bytes = bytes(await tg_file.download_as_bytearray())
        await _receive_voice_sample_from_video(update, video_bytes, video_filename, video_content_type)
        return
    if cur_state == "WAITING_TALKING_VIDEO_AUDIO":
        tg_file = await context.bot.get_file(doc.file_id)
        video_bytes = bytes(await tg_file.download_as_bytearray())
        await _receive_talking_video_audio_from_video(
            update,
            video_bytes,
            video_filename,
            video_content_type,
            getattr(doc, "duration", None),
        )
        return
    if cur_state == "WAITING_TALKING_VIDEO_IMAGE":
        await reply_autodelete(msg, "当前在等待图片，请直接发送要说话的人像图片。")
        return
    if cur_state == "WAITING_VOICE_TEXT":
        await reply_autodelete(msg, "当前在等待语音文案，请直接发送文字。")
        return
    if state and state.get("state") in WAITING_STATE_LABELS:
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
        return
    data = await load_data()
    if not get_user(data, user_id).get("api_key"):
        await reply_autodelete(msg, "⚠️ 请先设置 API Key：\n/key <你的 RunningHub API Key>")
        return

    tg_file = await context.bot.get_file(doc.file_id)
    video_bytes = bytes(await tg_file.download_as_bytearray())
    await _receive_video(update, context, video_bytes, video_filename, video_content_type)


async def _handle_audio_upload(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    file_id: str,
    filename: Optional[str],
    content_type: Optional[str],
    default_name: str,
    duration_seconds: Optional[float] = None,
):
    msg = update.message
    with contextlib.suppress(Exception):
        await context.bot.send_chat_action(chat_id=msg.chat_id, action=ChatAction.UPLOAD_DOCUMENT)

    user_id = msg.from_user.id
    audio_content_type = normalize_audio_content_type(content_type, filename)
    audio_filename = audio_filename_for_upload(filename, default_name, audio_content_type)
    tg_file = await context.bot.get_file(file_id)
    audio_bytes = bytes(await tg_file.download_as_bytearray())

    state = user_states.get(user_id)
    cur_state = state.get("state") if state else None
    if cur_state == "WAITING_VOICE_SAMPLE":
        await _receive_voice_sample(update, audio_bytes, audio_filename, audio_content_type)
        return
    if cur_state == "WAITING_TALKING_VIDEO_AUDIO":
        await _receive_talking_video_audio(
            update,
            audio_bytes,
            audio_filename,
            audio_content_type,
            duration_seconds,
        )
        return
    if cur_state == "WAITING_TALKING_VIDEO_IMAGE":
        await reply_autodelete(msg, "当前在等待图片，请直接发送要说话的人像图片。")
        return
    if cur_state == "WAITING_VOICE_TEXT":
        await reply_autodelete(msg, "当前在等待语音文案，请直接发送文字。")
        return
    if state and state.get("state") in WAITING_STATE_LABELS:
        await reply_autodelete(msg, build_waiting_state_conflict_message(state))
        return

    await reply_autodelete(
        msg,
        "要保存声音角色，请先发送 /savevoice <名称>，然后再发这段语音、音频或视频。",
    )


async def handle_voice_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_voice_message")
    voice = update.message.voice
    await _handle_audio_upload(
        update,
        context,
        voice.file_id,
        "voice.ogg",
        getattr(voice, "mime_type", None) or "audio/ogg",
        "voice.ogg",
        getattr(voice, "duration", None),
    )


async def handle_audio_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_audio_message")
    audio = update.message.audio
    await _handle_audio_upload(
        update,
        context,
        audio.file_id,
        getattr(audio, "file_name", None),
        getattr(audio, "mime_type", None),
        "audio.mp3",
        getattr(audio, "duration", None),
    )


async def handle_audio_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_audio_document")
    doc = update.message.document
    await _handle_audio_upload(
        update,
        context,
        doc.file_id,
        doc.file_name,
        doc.mime_type,
        "audio.mp3",
        None,
    )


# ─── InlineKeyboard 回调 ──────────────────────

async def _handle_video_outfit_callback(query, user_id: int):
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        await query.answer("选择信息无效，请重新发送视频。", show_alert=False)
        return

    _prefix, session_id, choice = parts
    session = pending_video_outfit_sessions.get(session_id)
    if not session or session.get("user_id") != user_id:
        await query.answer("这个视频已过期，请重新发送视频。", show_alert=False)
        return

    if choice == "upload":
        if session.get("processing"):
            await query.answer("🎬 视频换衣处理中，请等完成后再试。", show_alert=True)
            return
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_VIDEO_OUTFIT_IMAGE", "session_id": session_id, "flow_id": flow_id},
            "WAITING_VIDEO_OUTFIT_IMAGE",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        request_msg = await query.message.reply_text("👗 请发送衣服参考图（只参考身体/服装）：")
        session["video_ref_request_msg_id"] = request_msg.message_id
        await register_for_cleanup(query.message.chat_id, request_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_VIDEO_OUTFIT_IMAGE",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            request_msg.message_id,
            flow_id=flow_id,
        ))
        return

    if choice.startswith("img:"):
        if user_states.get(user_id, {}).get("state") in WAITING_STATE_LABELS:
            await query.answer(build_waiting_state_conflict_message(user_states[user_id]), show_alert=True)
            return
        if session.get("processing"):
            await query.answer("🎬 视频换衣处理中，请等完成后再试。", show_alert=True)
            return
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        mark_selection_clicked(session)
        try:
            img_idx = int(choice.split(":", 1)[1])
        except (ValueError, IndexError):
            await query.answer("预设信息无效。", show_alert=False)
            return
        image_preset_items = session.get("image_preset_items", [])
        if img_idx < 0 or img_idx >= len(image_preset_items):
            await query.answer("预设不存在，请重新发送视频。", show_alert=False)
            return
        preset_name, preset_path = image_preset_items[img_idx]
        video_bytes = await get_video_outfit_session_bytes(session)
        if not video_bytes:
            await reply_autodelete(query.message, "⚠️ 视频已过期，请重新发送视频。")
            return
        try:
            ref_bytes = await asyncio.to_thread(Path(preset_path).read_bytes)
        except Exception:
            await reply_autodelete(query.message, f"预设「{preset_name}」读取失败，可能已被删除。")
            return
        ref_content_type = image_content_type_from_bytes(ref_bytes, image_content_type_from_path(preset_path))
        data = await load_data()
        api_key = get_user(data, user_id).get("api_key")
        session["processing"] = True
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_video_outfit(
                query.message,
                api_key,
                video_bytes,
                ref_bytes,
                tg_user=query.from_user,
                video_filename=session.get("video_filename", "input.mp4"),
                video_content_type=session.get("video_content_type", DEFAULT_VIDEO_CONTENT_TYPE),
                ref_filename=image_filename_for_upload(preset_path, "reference", ref_content_type),
                ref_content_type=ref_content_type,
                video_session_id=session_id,
                video_session=session,
            ),
        )
        if not scheduled:
            session["processing"] = False
            await query.answer(build_user_task_limit_message(count), show_alert=True)
            return
        await query.answer("🎬 视频换衣处理中…")
        return

    await query.answer("选择信息无效，请重新发送视频。", show_alert=False)


def _ids_match(stored_value, current_value: int) -> bool:
    try:
        return int(stored_value) == int(current_value)
    except (TypeError, ValueError):
        return False


def _resolve_video_outfit_source(token: str, source: Optional[dict]) -> Optional[dict]:
    """解析源文件信息。内存失效时只使用带归属元数据的磁盘记录。"""
    if source:
        stored_path = source.get("path")
        if stored_path and Path(stored_path).exists():
            return source
        cleanup_pending_video_outfit_source(token, source)

    disk_source = read_video_outfit_source_meta(token)
    if disk_source:
        stored_path = disk_source.get("path")
        if stored_path and Path(stored_path).exists():
            pending_video_outfit_sources[token] = disk_source
            return disk_source
        cleanup_pending_video_outfit_source(token, disk_source)
    return None


async def _handle_video_outfit_download_callback(query, user_id: int):
    token = query.data.split(":", 1)[1] if ":" in query.data else ""
    if not re.fullmatch(r"[0-9a-f]{12}", token or ""):
        await query.answer("下载信息无效，请重新生成视频。", show_alert=False)
        return

    source = pending_video_outfit_sources.get(token)
    source_info = _resolve_video_outfit_source(token, source)
    if not source_info:
        await query.answer("源文件已过期，请重新生成视频。", show_alert=False)
        return

    chat_id = query.message.chat_id
    if not (
        _ids_match(source_info.get("user_id"), user_id)
        and _ids_match(source_info.get("chat_id"), chat_id)
    ):
        await query.answer("这个源文件不属于当前会话。", show_alert=True)
        return

    source_path = source_info.get("path")
    if not source_path:
        cleanup_pending_video_outfit_source(token, source_info)
        await query.answer("源文件已过期，请重新生成视频。", show_alert=False)
        return

    try:
        source_bytes = await asyncio.to_thread(Path(source_path).read_bytes)
    except Exception:
        cleanup_pending_video_outfit_source(token, source_info)
        await query.answer("源文件读取失败，请重新生成视频。", show_alert=False)
        return

    filename = source_info.get("filename") or Path(source_path).name
    try:
        sent = await reply_document_with_fallback(
            query.message,
            document=source_bytes,
            filename=filename,
            caption="📦 视频源文件已发送。",
        )
    except Exception:
        logger.exception("发送视频源文件失败：token=%s", token)
        await query.answer("发送失败，请稍后重试。", show_alert=True)
        return
    await register_for_cleanup(sent.chat_id, sent.message_id)
    await query.answer("已发送源文件", show_alert=False)


def _resolve_talking_video_audio(token: str, audio_info: Optional[dict]) -> Optional[dict]:
    if audio_info:
        stored_path = audio_info.get("path")
        if stored_path and Path(stored_path).exists():
            return audio_info
        cleanup_pending_talking_video_audio(token, audio_info)

    disk_audio = read_talking_video_audio_meta(token)
    if disk_audio:
        stored_path = disk_audio.get("path")
        if stored_path and Path(stored_path).exists():
            pending_talking_video_audios[token] = disk_audio
            return disk_audio
        cleanup_pending_talking_video_audio(token, disk_audio)
    return None


async def _handle_talking_video_audio_callback(query, user_id: int):
    token = query.data.split(":", 1)[1] if ":" in query.data else ""
    if not re.fullmatch(r"[0-9a-f]{12}", token or ""):
        await query.answer("音频信息无效，请重新生成语音。", show_alert=False)
        return

    audio_info = _resolve_talking_video_audio(token, pending_talking_video_audios.get(token))
    if not audio_info:
        await query.answer("音频已过期，请重新生成语音。", show_alert=False)
        return

    chat_id = query.message.chat_id
    if not (
        _ids_match(audio_info.get("user_id"), user_id)
        and _ids_match(audio_info.get("chat_id"), chat_id)
    ):
        await query.answer("这个音频不属于当前会话。", show_alert=True)
        return

    flow_id = uuid.uuid4().hex
    desired_state = {
        "state": "WAITING_TALKING_VIDEO_IMAGE",
        "save_id": token,
        "flow_id": flow_id,
        "audio_path": audio_info.get("path"),
        "audio_filename": audio_info.get("filename"),
        "audio_content_type": audio_info.get("content_type"),
        "duration_seconds": audio_info.get("duration_seconds"),
    }
    success, current_state, already_same = await claim_user_state(
        user_id,
        desired_state,
        "WAITING_TALKING_VIDEO_IMAGE",
        save_id=token,
    )
    if not success:
        await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
        return
    if already_same:
        await query.answer()
        return

    request_msg = await query.message.reply_text("🖼️ 请发送要说话的人像图片：")
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("flow_id") == flow_id:
            state["talking_video_image_request_msg_id"] = request_msg.message_id
    await register_for_cleanup(chat_id, request_msg.message_id)
    schedule_background_task(expire_waiting_flow_later(
        user_id,
        "WAITING_TALKING_VIDEO_IMAGE",
        "flow_id",
        flow_id,
        query.message.get_bot(),
        chat_id,
        request_msg.message_id,
        flow_id=flow_id,
    ))
    await query.answer()


async def _handle_voice_callback(query, user_id: int):
    action = query.data.split(":", 1)[1] if ":" in query.data else ""
    bot, chat_id, panel_msg_id = await cleanup_voice_panel_messages(query)
    if action == "cancel":
        await query.answer()
        return

    if user_states.get(user_id, {}).get("state") in WAITING_STATE_LABELS:
        await query.answer(build_waiting_state_conflict_message(user_states[user_id]), show_alert=True)
        return

    try:
        idx = int(action)
    except ValueError:
        await query.answer("选择信息无效。", show_alert=False)
        return

    data = await load_data()
    user = get_user(data, user_id)
    voice_preset_items = list(user.get("voice_presets", {}).items())
    if idx < 0 or idx >= len(voice_preset_items):
        await query.answer("声音角色不存在，请重新打开 /voice。", show_alert=False)
        return

    voice_name, voice_info_raw = voice_preset_items[idx]
    voice_info = normalize_voice_preset_info(voice_info_raw)
    voice_path = voice_info.get("path")
    if not voice_path or not Path(voice_path).exists():
        await query.answer("声音样本文件不存在，请重新保存。", show_alert=True)
        return

    flow_id = uuid.uuid4().hex
    success, current_state, already_same = await claim_user_state(
        user_id,
        {
            "state": "WAITING_VOICE_TEXT",
            "voice_name": voice_name,
            "voice_path": voice_path,
            "voice_filename": voice_info.get("filename") or Path(voice_path).name,
            "voice_content_type": voice_info.get("content_type") or audio_content_type_from_path(voice_path),
            "panel_msg_id": panel_msg_id,
            "flow_id": flow_id,
        },
        "WAITING_VOICE_TEXT",
    )
    if not success:
        await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
        return
    if already_same:
        await query.answer()
        return

    await query.answer()
    request_msg = await bot.send_message(chat_id=chat_id, text=f"🎙️ 已选择「{voice_name}」。请输入要生成的语音文案：")
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("flow_id") == flow_id:
            state["voice_text_request_msg_id"] = request_msg.message_id

    await register_for_cleanup(chat_id, request_msg.message_id)
    schedule_background_task(expire_waiting_flow_later(
        user_id,
        "WAITING_VOICE_TEXT",
        "flow_id",
        flow_id,
        bot,
        chat_id,
        request_msg.message_id,
        flow_id=flow_id,
    ))


async def _handle_animation_prompt_callback(query, user_id: int):
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        await query.answer("选择信息无效，请重新发图。", show_alert=False)
        return

    _prefix, session_id, choice = parts
    session = pending_prompt_sessions.get(session_id)
    if not session or session.get("user_id") != user_id:
        await query.answer("这张图或提示词已过期，请重新发图。", show_alert=False)
        return

    try:
        idx = int(choice)
    except ValueError:
        await query.answer("预设信息无效。", show_alert=False)
        return

    prompt_items = session.get("prompt_items", [])[:MAX_PROMPTS_SHOWN]
    if idx < 0 or idx >= len(prompt_items):
        await query.answer("提示词不存在，请重新发图。", show_alert=False)
        return

    _, prompt = prompt_items[idx]
    error = validate_prompt_text(prompt)
    if error:
        await query.answer(f"提示词不可用：{error}", show_alert=True)
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
        return

    current_state = await pop_user_state_if_same(user_id, "WAITING_ANIMATION_PROMPT", session_id=session_id)
    if not current_state:
        await query.answer("请先点击「生成动图」。", show_alert=False)
        return

    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")
    animation_seconds = normalize_animation_seconds(user)
    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_image_animation(
            query.message,
            api_key,
            image_bytes,
            prompt,
            tg_user=query.from_user,
            seconds=animation_seconds,
            image_filename=session.get("image_filename", "animation_input.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
        ),
    )
    if not scheduled:
        async with get_user_state_lock(user_id):
            if user_id not in user_states:
                user_states[user_id] = current_state
        await query.answer(build_user_task_limit_message(count), show_alert=True)
        return

    await query.answer("🎬 生成动图处理中…")
    with contextlib.suppress(Exception):
        await query.message.delete()


async def _handle_first_last_prompt_skip(query, user_id: int):
    """点击「跳过」→ 用用户默认提示词触发首尾视频。"""
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        await query.answer("选择信息无效。", show_alert=False)
        return
    _, session_id, action = parts
    if action != "skip":
        await query.answer("未知操作", show_alert=False)
        return
    session = pending_prompt_sessions.get(session_id)
    if not session or session.get("user_id") != user_id:
        await query.answer("会话已过期，请重新发图。", show_alert=False)
        return

    current_state = await pop_user_state_if_same(user_id, "WAITING_FIRST_LAST_PROMPT", session_id=session_id)
    if not current_state:
        await query.answer("请先点击「首尾视频」。", show_alert=False)
        return

    data = await load_data()
    user = get_user(data, user_id)
    prompt = get_user_first_last_prompt(user)

    first_last_prompt_request_msg_id = session.pop("first_last_prompt_request_msg_id", None)
    for mid in filter(None, [query.message.message_id, first_last_prompt_request_msg_id]):
        with contextlib.suppress(Exception):
            await query.message.get_bot().delete_message(query.message.chat_id, mid)

    started = await _start_first_last_video(
        query.message, user_id, session_id, session, prompt, tg_user=query.from_user,
    )
    if not started:
        # 失败时把状态还回去，避免用户再发一次还是被堵
        async with get_user_state_lock(user_id):
            if user_id not in user_states:
                user_states[user_id] = current_state
        return
    await query.answer("🎞️ 已用默认提示词启动")


async def _handle_ds_callback(query, user_id: int):
    """处理 DeepSeek 确定 / 重试 / 取消 回调。"""
    parts = query.data.split(":", 2)
    if len(parts) != 3:
        await query.answer("无效操作", show_alert=False)
        return

    _, ds_sid, action = parts
    ds_entry = ds_pending.get(ds_sid)
    if not ds_entry or ds_entry.get("user_id") != user_id:
        await query.answer("已过期，请重新生成。", show_alert=False)
        with contextlib.suppress(Exception):
            await query.message.delete()
        return

    session_id = ds_entry.get("session_id")
    session    = pending_prompt_sessions.get(session_id)
    if not session:
        await query.answer("原图已过期，请重新发图。", show_alert=False)
        ds_pending.pop(ds_sid, None)
        with contextlib.suppress(Exception):
            await query.message.delete()
        return

    if action == "cancel":
        if ds_entry.get("processing"):
            await query.answer("任务已开始处理，请稍候。", show_alert=False)
            return
        await query.answer()
        ds_pending.pop(ds_sid, None)
        with contextlib.suppress(Exception):
            await query.message.delete()
        return

    if action == "retry":
        if ds_entry.get("processing"):
            await query.answer("任务已开始处理，请稍候。", show_alert=False)
            return
        if ds_entry.get("generating"):
            await query.answer("正在重新生成，请稍候。", show_alert=False)
            return
        ds_entry["generating"] = True
        await query.answer()
        await safe_edit_text(query.message, "🤖 重新生成中…", log_context="回调消息")
        ds_user_data = await load_data()
        ds_user_prompt = get_user(ds_user_data, user_id).get("ds_prompt")
        try:
            generated = await ds_generate_prompt(user_id, user_system_prompt=ds_user_prompt)
        except Exception as e:
            latest_entry = ds_pending.get(ds_sid)
            if latest_entry and latest_entry.get("user_id") == user_id:
                latest_entry.pop("generating", None)
            await safe_edit_text(query.message, f"❌ 生成失败：{e}", log_context="回调消息")
            return
        latest_entry = ds_pending.get(ds_sid)
        if not latest_entry or latest_entry.get("user_id") != user_id:
            with contextlib.suppress(Exception):
                await query.message.delete()
            return
        latest_entry["prompt"] = generated
        latest_entry.pop("generating", None)
        expire_token = uuid.uuid4().hex
        latest_entry["expire_token"] = expire_token
        prompt_msg = await edit_or_reply_ds_prompt_message(
            query.message,
            ds_sid,
            generated,
            log_context="回调消息",
        )
        schedule_background_task(_expire_ds_confirm_later(
            ds_sid,
            expire_token,
            prompt_msg.get_bot(),
            prompt_msg.chat_id,
            prompt_msg.message_id,
        ))
        return

    if action == "confirm":
        expired = False
        already_processing = False
        still_generating = False
        validation_error = None
        prompt = ""
        async with get_user_state_lock(user_id):
            latest_entry = ds_pending.get(ds_sid)
            if not latest_entry or latest_entry.get("user_id") != user_id:
                expired = True
            elif latest_entry.get("processing"):
                already_processing = True
            elif latest_entry.get("generating"):
                still_generating = True
            else:
                prompt = (latest_entry.get("prompt") or "").strip()
                validation_error = validate_prompt_text(prompt)
                if not validation_error:
                    latest_entry["processing"] = True

        if expired:
            await query.answer("已过期，请重新生成。", show_alert=False)
            with contextlib.suppress(Exception):
                await query.message.delete()
            return
        if already_processing:
            await query.answer("任务已开始处理，请稍候。", show_alert=False)
            return
        if still_generating:
            await query.answer("正在重新生成，请稍候。", show_alert=False)
            return
        if validation_error:
            await query.answer(f"提示词不可用：{validation_error}", show_alert=True)
            return

        image_bytes = await get_session_image_bytes(session)
        if not image_bytes:
            async with get_user_state_lock(user_id):
                latest_entry = ds_pending.get(ds_sid)
                if latest_entry and latest_entry.get("user_id") == user_id:
                    latest_entry["processing"] = False
            await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
            return
        data = await load_data()
        user = get_user(data, user_id)
        api_key    = user.get("api_key")

        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_image(
                query.message,
                api_key,
                image_bytes,
                prompt,
                tg_user=query.from_user,
                image_filename=session.get("image_filename", "input.jpg"),
                image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ),
        )
        if not scheduled:
            async with get_user_state_lock(user_id):
                latest_entry = ds_pending.get(ds_sid)
                if latest_entry and latest_entry.get("user_id") == user_id:
                    latest_entry["processing"] = False
            await query.answer(build_user_task_limit_message(count), show_alert=True)
            return

        ds_pending.pop(ds_sid, None)
        await query.answer()
        with contextlib.suppress(Exception):
            await query.message.delete()
        return


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_callback")
    query   = update.callback_query
    user_id = query.from_user.id

    if query.data.startswith("outfit:save:"):
        save_id = query.data.split(":", 2)[2]
        save_info = pending_preset_saves.get(save_id)
        if not save_info or save_info.get("user_id") != user_id:
            await query.answer("图片已过期，无法保存。", show_alert=False)
            return
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_PRESET_NAME", "save_id": save_id},
            "WAITING_PRESET_NAME",
            save_id=save_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        await query.answer()
        ask_msg = await query.message.reply_text("💾 请输入预设名称：")
        save_info["ask_msg_id"] = ask_msg.message_id
        # 5 分钟无操作：删引导消息 + 清状态，保存按钮留着可再次点击
        schedule_background_task(expire_preset_save_later(
            save_id, query.message.get_bot(), query.message.chat_id, ask_msg.message_id, user_id
        ))
        return

    if query.data.startswith("presetflow:"):
        workflow = query.data.split(":", 1)[1]
        if workflow not in PRESET_WORKFLOW_KEYS:
            await query.answer("无效选项", show_alert=False)
            return
        def _mutate_pf(data: dict):
            get_user(data, user_id)["preset_workflow"] = workflow
        await update_data(_mutate_pf)
        label = "红火" if workflow == "firered" else "千问"
        await query.answer()
        await safe_edit_text(
            query.message,
            f"✅ 参考换衣工作流已切换为 {label}。",
            log_context="回调消息",
            reply_markup=None,
        )
        schedule_background_task(delete_message_later(
            query.get_bot(), query.message.chat_id, query.message.message_id, WARNING_DELETE_SECONDS
        ))
        pending = presetflow_pending.pop(user_id, None)
        if pending:
            with contextlib.suppress(Exception):
                await query.get_bot().delete_message(pending["chat_id"], pending["cmd_msg_id"])
        return

    if query.data.startswith("reset:"):
        action = query.data.split(":", 1)[1]
        if action == "confirm":
            # 先回调，避免按钮一直转圈；后续清理放在当前协程里完成。
            await query.answer()
            def _mutate(data: dict):
                user = get_user(data, user_id)
                api_key = user.get("api_key")
                image_paths = list(user.get("image_presets", {}).values())
                voice_paths = [
                    voice_preset_path(info)
                    for info in user.get("voice_presets", {}).values()
                ]
                data["users"][str(user_id)] = {"api_key": api_key, "prompts": {}}
                return image_paths, voice_paths

            image_paths, voice_paths = await update_data(_mutate)
            for path in image_paths:
                cleanup_temp_file(path)
            for path in filter(None, voice_paths):
                cleanup_temp_file(path)
            with contextlib.suppress(Exception):
                shutil.rmtree(PRESET_IMAGE_DIR / str(user_id))
            with contextlib.suppress(Exception):
                shutil.rmtree(VOICE_PRESET_DIR / str(user_id))
            ds_histories.pop(user_id, None)
            async with get_user_state_lock(user_id):
                user_states.pop(user_id, None)
            for save_id, info in list(pending_preset_saves.items()):
                if info.get("user_id") == user_id:
                    cleanup_pending_preset_file(pending_preset_saves.pop(save_id, None))
            for sid, info in list(pending_video_outfit_sessions.items()):
                if info.get("user_id") == user_id:
                    cleanup_temp_file(info.get("video_path"))
                    pending_video_outfit_sessions.pop(sid, None)
            for ds_sid, info in list(ds_pending.items()):
                if info.get("user_id") == user_id:
                    ds_pending.pop(ds_sid, None)
            await safe_edit_text(query.message, "✅ 已清空，API Key 保留。", log_context="回调消息", reply_markup=None)
            schedule_background_task(delete_message_later(
                query.get_bot(), query.message.chat_id, query.message.message_id, WARNING_DELETE_SECONDS
            ))
        elif action == "cancel":
            await query.answer()
            bot = query.get_bot()
            chat_id = query.message.chat_id
            orig = query.message.reply_to_message
            if orig:
                with contextlib.suppress(Exception):
                    await bot.delete_message(chat_id, orig.message_id)
            with contextlib.suppress(Exception):
                await bot.delete_message(chat_id, query.message.message_id)
        return

    if query.data.startswith("start:"):
        action = query.data.split(":", 1)[1]
        if action == "confirm":
            await query.answer()
            data = await load_data()
            user = get_user(data, user_id)
            await safe_edit_text(query.message, build_help_center(user), log_context="回调消息", reply_markup=None)
        elif action == "cancel":
            await query.answer()
            await safe_edit_text(query.message, "已取消，随时发送 /start 重新开始。", log_context="回调消息", reply_markup=None)
        return

    if query.data.startswith("compare:"):
        key = query.data.split(":", 1)[1]
        if key not in ("faceswap", "bodyswap", "custom"):
            await query.answer("无效选项", show_alert=False)
            return
        def _mutate(data: dict):
            user = get_user(data, user_id)
            switches = user.setdefault("compare_switches", {})
            switches[key] = not switches.get(key)
            return switches.get(key)
        new_val = await update_data(_mutate)
        await query.answer(f"已{'开启' if new_val else '关闭'}")
        # 刷新面板
        data = await load_data()
        user = get_user(data, user_id)
        switches = user.get("compare_switches", {})
        def _label(k, n):
            return f"{'✅' if switches.get(k) else '❌'} {n}"
        new_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(_label("faceswap", "参考图换脸"), callback_data="compare:faceswap")],
            [InlineKeyboardButton(_label("bodyswap", "参考换衣"), callback_data="compare:bodyswap")],
            [InlineKeyboardButton(_label("custom", "自定义提示"), callback_data="compare:custom")],
        ])
        with contextlib.suppress(Exception):
            await query.message.edit_reply_markup(reply_markup=new_kb)
        return

    if query.data.startswith("expand:"):
        direction = query.data.split(":", 1)[1]
        if direction not in IMAGE_EXTEND_DIRECTIONS:
            await query.answer("无效选项", show_alert=False)
            return

        def _mutate(data: dict):
            user = get_user(data, user_id)
            switches = normalize_image_extend_switches(user)
            switches[direction] = not switches.get(direction, True)
            return switches[direction]

        new_val = await update_data(_mutate)
        await query.answer(f"{IMAGE_EXTEND_LABELS[direction]} 已{'开启' if new_val else '关闭'}")

        data = await load_data()
        user = get_user(data, user_id)
        with contextlib.suppress(Exception):
            await query.message.edit_reply_markup(reply_markup=build_image_extend_keyboard(user))
        return

    if query.data.startswith("videooutfitdl:"):
        await _handle_video_outfit_download_callback(query, user_id)
        return

    if query.data.startswith("voice:"):
        await _handle_voice_callback(query, user_id)
        return

    if query.data.startswith("talkvid:"):
        await _handle_talking_video_audio_callback(query, user_id)
        return

    if query.data.startswith("videooutfit:"):
        await _handle_video_outfit_callback(query, user_id)
        return

    if query.data.startswith("ds:"):
        await _handle_ds_callback(query, user_id)
        return

    if query.data.startswith("animation:"):
        await _handle_animation_prompt_callback(query, user_id)
        return

    if query.data.startswith("firstlastprompt:"):
        await _handle_first_last_prompt_skip(query, user_id)
        return

    if not query.data.startswith("prompt:"):
        return

    parts = query.data.split(":", 2)
    if len(parts) != 3:
        await query.answer("选择信息无效，请重新发图。", show_alert=False)
        return

    _prefix, session_id, choice = parts
    session = pending_prompt_sessions.get(session_id)
    if not session or session.get("user_id") != user_id:
        await query.answer("这张图或提示词已过期，请重新发图。", show_alert=False)
        return

    # 图片预设选择 → 参考图换身体
    if choice.startswith("img:"):
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        mark_selection_clicked(session)
        try:
            img_idx = int(choice.split(":", 1)[1])
        except (ValueError, IndexError):
            await query.answer("预设信息无效。", show_alert=False)
            return
        image_preset_items = session.get("image_preset_items", [])
        if img_idx < 0 or img_idx >= len(image_preset_items):
            await query.answer("预设不存在，请重新发图。", show_alert=False)
            return
        preset_name, preset_path = image_preset_items[img_idx]
        image_bytes = await get_session_image_bytes(session)
        if not image_bytes:
            await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
            return
        try:
            ref_bytes = await asyncio.to_thread(Path(preset_path).read_bytes)
        except Exception:
            await reply_autodelete(query.message, "预设图片读取失败，可能已被删除。")
            return
        ref_content_type = image_content_type_from_bytes(ref_bytes, image_content_type_from_path(preset_path))
        data = await load_data()
        user_cfg = get_user(data, user_id)
        api_key = user_cfg.get("api_key")
        preset_workflow = user_cfg.get("preset_workflow", DEFAULT_PRESET_WORKFLOW_KEY)
        process_fn = process_qwen_outfit if preset_workflow == "qwen" else process_bodyswap
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_fn(
                query.message,
                api_key,
                image_bytes,
                ref_bytes,
                tg_user=query.from_user,
                image_filename=session.get("image_filename", "original.jpg"),
                image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
                ref_filename=image_filename_for_upload(preset_path, "reference", ref_content_type),
                ref_content_type=ref_content_type,
            ),
        )
        if not scheduled:
            await query.answer(build_user_task_limit_message(count), show_alert=True)
            return
        await query.answer()
        return

    # 首尾视频：引导发送尾帧图片
    if choice == "firstlast":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_LAST_FRAME_IMAGE", "session_id": session_id, "flow_id": flow_id},
            "WAITING_LAST_FRAME_IMAGE",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        last_frame_msg = await query.message.reply_text("🎞️ 请发送尾帧图片：")
        session["last_frame_request_msg_id"] = last_frame_msg.message_id
        await register_for_cleanup(query.message.chat_id, last_frame_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_LAST_FRAME_IMAGE",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            last_frame_msg.message_id,
            flow_id=flow_id,
        ))
        return

    # 图片扩展：按 /expand 面板保存的方向开关扩展图片边缘。
    if choice == "extend":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        mark_selection_clicked(session)
        image_bytes = await get_session_image_bytes(session)
        if not image_bytes:
            await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
            return
        data = await load_data()
        user_cfg = get_user(data, user_id)
        api_key = user_cfg.get("api_key")
        extend_values = build_image_extend_values(user_cfg)
        if all(value == 0 for value in extend_values.values()):
            await query.answer("四边都关闭了，请用 /expand 开启至少一边。", show_alert=True)
            return
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_image_extend(
                query.message,
                api_key,
                image_bytes,
                extend_values,
                tg_user=query.from_user,
                image_filename=session.get("image_filename", "extend_input.jpg"),
                image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ),
        )
        if not scheduled:
            await query.answer(build_user_task_limit_message(count), show_alert=True)
            return
        await query.answer("🖼️ 图片扩展处理中…")
        return

    # 提取图片服装
    if choice == "outfit":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        mark_selection_clicked(session)
        image_bytes = await get_session_image_bytes(session)
        if not image_bytes:
            await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
            return
        data = await load_data()
        api_key = get_user(data, user_id).get("api_key")
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_outfit(
                query.message,
                api_key,
                image_bytes,
                user_id,
                tg_user=query.from_user,
                image_filename=session.get("image_filename", "outfit_input.jpg"),
                image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ),
        )
        if not scheduled:
            await query.answer(build_user_task_limit_message(count), show_alert=True)
            return
        await query.answer()
        return

    # DeepSeek 随机生成
    if choice == "dsgen":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        mark_selection_clicked(session)
        await query.answer()
        gen_msg = await query.message.reply_text("🤖 生成中…")
        ds_user_data = await load_data()
        ds_user_prompt = get_user(ds_user_data, user_id).get("ds_prompt")
        try:
            generated = await ds_generate_prompt(user_id, user_system_prompt=ds_user_prompt)
        except Exception as e:
            await safe_edit_text(gen_msg, f"❌ 生成失败：{e}", log_context="生成消息", reply_markup=None)
            schedule_background_task(
                delete_message_later(gen_msg.get_bot(), gen_msg.chat_id, gen_msg.message_id, WARNING_DELETE_SECONDS)
            )
            return
        ds_sid = uuid.uuid4().hex[:10]
        expire_token = uuid.uuid4().hex
        ds_pending[ds_sid] = {
            "prompt": generated,
            "session_id": session_id,
            "user_id": user_id,
            "expire_token": expire_token,
        }
        prompt_msg = await edit_or_reply_ds_prompt_message(
            gen_msg,
            ds_sid,
            generated,
            log_context="生成消息",
        )
        schedule_background_task(_expire_ds_confirm_later(
            ds_sid, expire_token, prompt_msg.get_bot(), prompt_msg.chat_id, prompt_msg.message_id,
        ))
        return

    if choice == "scene_replace":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_SCENE_PERSON_IMAGE", "session_id": session_id, "flow_id": flow_id},
            "WAITING_SCENE_PERSON_IMAGE",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        scene_person_msg = await query.message.reply_text("🧍 请发送人物图（会连同衣服一起换到当前场景中）：")
        session["scene_person_request_msg_id"] = scene_person_msg.message_id
        await register_for_cleanup(query.message.chat_id, scene_person_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_SCENE_PERSON_IMAGE",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            scene_person_msg.message_id,
            flow_id=flow_id,
        ))
        return

    if choice == "gif":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_ANIMATION_PROMPT", "session_id": session_id, "flow_id": flow_id},
            "WAITING_ANIMATION_PROMPT",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        animation_prompt_msg = await query.message.reply_text(
            "🎬 请输入动图提示词：",
            reply_markup=build_animation_prompt_keyboard(session_id, session.get("prompt_items", [])),
        )
        session["animation_prompt_request_msg_id"] = animation_prompt_msg.message_id
        await register_for_cleanup(query.message.chat_id, animation_prompt_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_ANIMATION_PROMPT",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            animation_prompt_msg.message_id,
            flow_id=flow_id,
        ))
        return

    if choice == "talking_video":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_TALKING_VIDEO_AUDIO", "session_id": session_id, "flow_id": flow_id},
            "WAITING_TALKING_VIDEO_AUDIO",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        talking_audio_msg = await query.message.reply_text("🎙️ 请发送音频文件、语音，或带声音的视频：")
        session["talking_video_audio_request_msg_id"] = talking_audio_msg.message_id
        await register_for_cleanup(query.message.chat_id, talking_audio_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_TALKING_VIDEO_AUDIO",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            talking_audio_msg.message_id,
            flow_id=flow_id,
        ))
        return

    # 换脸：引导发送脸图
    if choice == "faceswap":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_FACE_IMAGE", "session_id": session_id, "flow_id": flow_id},
            "WAITING_FACE_IMAGE",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        face_request_msg = await query.message.reply_text("🪑 请发送要换成的脸部图片：")
        session["face_request_msg_id"] = face_request_msg.message_id
        await register_for_cleanup(query.message.chat_id, face_request_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_FACE_IMAGE",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            face_request_msg.message_id,
            flow_id=flow_id,
        ))
        return

    # 参考换衣：引导发送参考图；旧 refimg/qwenimg callback 兼容到同一入口。
    if choice in ("refoutfit", "refimg", "qwenimg"):
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_BODY_REF_IMAGE", "session_id": session_id, "flow_id": flow_id},
            "WAITING_BODY_REF_IMAGE",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        body_request_msg = await query.message.reply_text("👗 请发送参考服装图片：")
        session["body_request_msg_id"] = body_request_msg.message_id
        await register_for_cleanup(query.message.chat_id, body_request_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_BODY_REF_IMAGE",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            body_request_msg.message_id,
            flow_id=flow_id,
        ))
        return

    # 自定义提示词：进入等待文字输入状态
    if choice == "custom":
        if is_selection_rate_limited(session):
            await query.answer("操作频繁，请稍后重试", show_alert=False)
            return
        flow_id = uuid.uuid4().hex
        success, current_state, already_same = await claim_user_state(
            user_id,
            {"state": "WAITING_CUSTOM_PROMPT", "session_id": session_id, "flow_id": flow_id},
            "WAITING_CUSTOM_PROMPT",
            session_id=session_id,
        )
        if not success:
            await query.answer(build_waiting_state_conflict_message(current_state), show_alert=True)
            return
        if already_same:
            await query.answer()
            return
        mark_selection_clicked(session)
        await query.answer()
        prompt_request_msg = await query.message.reply_text("✏️ 请输入自定义提示词：")
        session["prompt_request_msg_id"] = prompt_request_msg.message_id
        await register_for_cleanup(query.message.chat_id, prompt_request_msg.message_id)
        schedule_background_task(expire_waiting_flow_later(
            user_id,
            "WAITING_CUSTOM_PROMPT",
            "session_id",
            session_id,
            query.message.get_bot(),
            query.message.chat_id,
            prompt_request_msg.message_id,
            flow_id=flow_id,
        ))
        return

    prompt_items = session.get("prompt_items", [])
    try:
        idx = int(choice)
    except ValueError:
        await query.answer("选择信息无效，请重新发图。", show_alert=False)
        return

    if idx < 0 or idx >= len(prompt_items):
        await query.answer("提示词不存在，请重新发图。", show_alert=False)
        return

    if is_selection_rate_limited(session):
        await query.answer("操作频繁，请稍后重试", show_alert=False)
        return

    _, prompt = prompt_items[idx]
    mark_selection_clicked(session)

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(query.message, "⚠️ 会话已过期，请重新上传图片。")
        return
    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")
    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_image(
            query.message,
            api_key,
            image_bytes,
            prompt,
            tg_user=query.from_user,
            image_filename=session.get("image_filename", "input.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
        ),
    )
    if not scheduled:
        await query.answer(build_user_task_limit_message(count), show_alert=True)
        return
    # 只清文字预设相关的等待状态（CUSTOM_PROMPT 或 无状态），不误伤其他流程
    async with get_user_state_lock(user_id):
        cur = user_states.get(user_id)
        if cur is None or cur.get("state") in (None, "WAITING_CUSTOM_PROMPT"):
            user_states.pop(user_id, None)
    await query.answer()


# ─── 文字输入 Handler（自定义提示词）─────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "handle_text")
    user_id = update.effective_user.id

    state = user_states.get(user_id)
    if not state:
        data = await load_data()
        user = get_user(data, user_id)
        await update.message.reply_text(build_help_center(user))
        return

    # ── 声音克隆文案输入 ─────────────────────────
    if state.get("state") == "WAITING_VOICE_TEXT":
        text = update.message.text.strip()
        error = validate_voice_text(text)
        if error:
            await reply_autodelete(update.message, f"❌ {error}", also_delete=update.message)
            return

        voice_path = state.get("voice_path")
        if not voice_path:
            await reply_autodelete(update.message, "⚠️ 声音角色已失效，请重新打开 /voice。", also_delete=update.message)
            async with get_user_state_lock(user_id):
                if user_states.get(user_id) is state:
                    user_states.pop(user_id, None)
            return

        if await is_user_task_limit_reached(user_id):
            await reply_autodelete(update.message, build_user_task_limit_message(), also_delete=update.message)
            return

        current_state = await pop_user_state_if_same(user_id, "WAITING_VOICE_TEXT")
        if not current_state:
            return

        try:
            sample_bytes = await asyncio.to_thread(Path(voice_path).read_bytes)
        except Exception:
            await reply_autodelete(update.message, "⚠️ 声音样本读取失败，请重新保存这个声音角色。", also_delete=update.message)
            return

        data = await load_data()
        user = get_user(data, user_id)
        api_key = user.get("api_key")
        voice_name = current_state.get("voice_name") or "未命名"
        voice_filename = current_state.get("voice_filename") or Path(voice_path).name
        voice_content_type = current_state.get("voice_content_type") or audio_content_type_from_bytes(
            sample_bytes,
            audio_content_type_from_path(voice_path),
        )

        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_voice_clone(
                update.message,
                api_key,
                sample_bytes,
                text,
                voice_name,
                tg_user=update.message.from_user,
                sample_filename=audio_filename_for_upload(voice_filename, "voice_sample", voice_content_type),
                sample_content_type=voice_content_type,
            ),
        )
        if not scheduled:
            async with get_user_state_lock(user_id):
                if user_id not in user_states:
                    user_states[user_id] = current_state
            await reply_autodelete(update.message, build_user_task_limit_message(count), also_delete=update.message)
            return

        for mid in filter(None, [
            update.message.message_id,
            current_state.get("voice_text_request_msg_id"),
            current_state.get("panel_msg_id"),
        ]):
            with contextlib.suppress(Exception):
                await update.message.get_bot().delete_message(update.message.chat_id, mid)
        return

    # ── 对比图文案输入（第一步：原图文案）──────────────
    if state.get("state") == "WAITING_COMPARE_ORIG":
        text = update.message.text.strip()
        if not text:
            await reply_autodelete(update.message, "文案不能为空，请重新输入：", also_delete=update.message)
            return
        if len(text) > 6:
            text = text[:6]
        current_state = await pop_user_state_if_same(user_id, "WAITING_COMPARE_ORIG")
        if not current_state:
            return
        # 保存原图文案
        def _mutate(data: dict):
            get_user(data, user_id)["compare_origin_text"] = text
        await update_data(_mutate)
        # 删除 step1 的引导消息和用户输入
        bot = update.message.get_bot()
        chat_id = update.message.chat_id
        await bot.delete_message(chat_id, update.message.message_id)
        step1_prompt_id = current_state.get("_prompt_msg_id")
        if step1_prompt_id:
            with contextlib.suppress(Exception):
                await bot.delete_message(chat_id, step1_prompt_id)
        # 进入第二步：引导输入结果文案
        flow_id2 = uuid.uuid4().hex
        success2, _, _ = await claim_user_state(
            user_id,
            {"state": "WAITING_COMPARE_RESULT", "flow_id": flow_id2},
            "WAITING_COMPARE_RESULT",
        )
        if not success2:
            await reply_autodelete(update.message, "✅ 原图文案已保存，但设置失败，请重试 /comparetext。", also_delete=update.message)
            return
        prompt_msg = await update.message.reply_text("✅ 原图文案已保存。请输入对比图右边的编辑后文案（最多6字）：")
        # 存 step2 的引导消息 ID
        async with get_user_state_lock(user_id):
            state2 = user_states.get(user_id)
            if state2 and state2.get("flow_id") == flow_id2:
                state2["_prompt_msg_id"] = prompt_msg.message_id
                state2["_chat_id"] = chat_id
        schedule_background_task(delete_message_later(prompt_msg.get_bot(), chat_id, prompt_msg.message_id, WAITING_FLOW_TIMEOUT_SECONDS))
        schedule_background_task(expire_waiting_flow_later(
            user_id, "WAITING_COMPARE_RESULT", "flow_id", flow_id2,
            prompt_msg.get_bot(), chat_id, prompt_msg.message_id,
            flow_id=flow_id2,
        ))
        return

    # ── 对比图文案输入（第二步：结果文案）──────────────
    if state.get("state") == "WAITING_COMPARE_RESULT":
        text = update.message.text.strip()
        if not text:
            await reply_autodelete(update.message, "文案不能为空，请重新输入：", also_delete=update.message)
            return
        if len(text) > 6:
            text = text[:6]
        current_state = await pop_user_state_if_same(user_id, "WAITING_COMPARE_RESULT")
        if not current_state:
            return
        def _mutate(data: dict):
            user = get_user(data, user_id)
            user["compare_result_text"] = text
            return user.get("compare_origin_text", "").strip()
        origin_text = await update_data(_mutate) or "原图"
        # 立即删用户输入、step2 引导消息、和原始 /comparetext 命令消息
        bot = update.message.get_bot()
        chat_id = update.message.chat_id
        await bot.delete_message(chat_id, update.message.message_id)
        step2_prompt_id = current_state.get("_prompt_msg_id")
        if step2_prompt_id:
            with contextlib.suppress(Exception):
                await bot.delete_message(chat_id, step2_prompt_id)
        cmd_msg_id = current_state.get("_cmd_msg_id")
        if cmd_msg_id:
            with contextlib.suppress(Exception):
                await bot.delete_message(chat_id, cmd_msg_id)
        # 确认消息5秒后删
        sent = await update.message.reply_text(f"✅ 对比图文案已设置！\n左边：{origin_text}\n右边：{text}")
        schedule_background_task(delete_message_later(sent.get_bot(), sent.chat_id, sent.message_id, WARNING_DELETE_SECONDS))
        return

    # ── 保存图片预设名称 ──────────────────────────
    if state.get("state") == "WAITING_PRESET_NAME":
        preset_name = update.message.text.strip()
        if not preset_name:
            await reply_autodelete(update.message, "名称不能为空，请重新输入：", also_delete=update.message)
            return

        save_id = state.get("save_id")
        current_state = await pop_user_state_if_same(user_id, "WAITING_PRESET_NAME", save_id=save_id)
        if not current_state:
            return
        save_info = pending_preset_saves.pop(save_id, None) if save_id else None

        if not save_info:
            await reply_autodelete(update.message, "⚠️ 图片已过期，请重新提取服装。", also_delete=update.message)
            return

        preset_bytes = await read_pending_preset_bytes(save_info)
        if not preset_bytes:
            cleanup_pending_preset_file(save_info)
            await reply_autodelete(update.message, "⚠️ 图片已过期，请重新提取服装。", also_delete=update.message)
            return

        # 保存图片到本地
        PRESET_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
        user_preset_dir = PRESET_IMAGE_DIR / str(user_id)
        user_preset_dir.mkdir(parents=True, exist_ok=True)
        save_path = build_image_preset_save_path(
            user_id,
            preset_name,
            save_info.get("content_type"),
            save_info.get("filename"),
            unique_id=save_id,
        )
        await asyncio.to_thread(save_path.write_bytes, preset_bytes)
        cleanup_pending_preset_file(save_info)

        def _mutate(data: dict):
            user = get_user(data, user_id)
            image_presets = user.setdefault("image_presets", {})
            old_path = image_presets.get(preset_name)
            image_presets[preset_name] = str(save_path)
            return old_path

        old_path = await update_data(_mutate)
        if old_path and old_path != str(save_path):
            await cleanup_image_preset_file_if_unreferenced(old_path)

        bot = update.message.get_bot()
        chat_id = update.message.chat_id

        # 保存成功：撤掉结果图上的保存按钮
        result_msg_id = save_info.get("result_msg_id")
        if result_msg_id:
            with contextlib.suppress(Exception):
                await bot.edit_message_reply_markup(
                    chat_id=save_info.get("chat_id") or chat_id,
                    message_id=result_msg_id,
                    reply_markup=None,
                )

        # 清理引导消息和用户输入
        ask_msg_id = save_info.get("ask_msg_id")
        for mid in filter(None, [ask_msg_id, update.message.message_id]):
            with contextlib.suppress(Exception):
                await bot.delete_message(chat_id, mid)

        await send_autodelete_message(bot, chat_id, f"✅ 图片预设「{preset_name}」已保存。")
        return

    if state.get("state") == "WAITING_IMAGE_PRESET_IMAGE":
        await reply_autodelete(update.message, "请发送图片来保存为图片预设。", also_delete=update.message)
        return

    if state.get("state") == "WAITING_VOICE_SAMPLE":
        await reply_autodelete(update.message, "请发送语音、音频文件或带声音的视频来保存声音角色。", also_delete=update.message)
        return

    if state.get("state") == "WAITING_SCENE_PERSON_IMAGE":
        await reply_autodelete(update.message, "请发送人物图片来进行场景换人。", also_delete=update.message)
        return

    if state.get("state") == "WAITING_TALKING_VIDEO_AUDIO":
        await reply_autodelete(update.message, "请发送音频文件、语音，或带声音的视频来生成说话视频。", also_delete=update.message)
        return

    if state.get("state") == "WAITING_TALKING_VIDEO_IMAGE":
        await reply_autodelete(update.message, "请发送要说话的人像图片。", also_delete=update.message)
        return

    if state.get("state") == "WAITING_ANIMATION_PROMPT":
        prompt = update.message.text.strip()
        if not prompt:
            await reply_autodelete(update.message, "提示词不能为空，请重新输入：", also_delete=update.message)
            return

        error = validate_prompt_text(prompt)
        if error:
            warning_message = await update.message.reply_text(build_prompt_validation_reply(error))
            schedule_background_task(delete_message_later(update.message.get_bot(), update.message.chat_id, update.message.message_id, WARNING_DELETE_SECONDS))
            schedule_background_task(delete_message_later(warning_message.get_bot(), warning_message.chat_id, warning_message.message_id, WARNING_DELETE_SECONDS))
            return

        session_id = state.get("session_id")
        if await is_user_task_limit_reached(user_id):
            await reply_autodelete(update.message, build_user_task_limit_message(), also_delete=update.message)
            return

        current_state = await pop_user_state_if_same(user_id, "WAITING_ANIMATION_PROMPT", session_id=session_id)
        if not current_state:
            return

        session = pending_prompt_sessions.get(session_id) if session_id else None
        if not session:
            await reply_autodelete(update.message, "⚠️ 图片已过期，请重新发图。", also_delete=update.message)
            return

        image_bytes = await get_session_image_bytes(session)
        if not image_bytes:
            await reply_autodelete(update.message, "⚠️ 会话已过期，请重新上传图片。", also_delete=update.message)
            return

        data = await load_data()
        user = get_user(data, user_id)
        api_key = user.get("api_key")
        animation_seconds = normalize_animation_seconds(user)
        scheduled, count = await schedule_user_processing_task(
            user_id,
            lambda: process_image_animation(
                update.message,
                api_key,
                image_bytes,
                prompt,
                seconds=animation_seconds,
                image_filename=session.get("image_filename", "animation_input.jpg"),
                image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
            ),
        )
        if not scheduled:
            await reply_autodelete(update.message, build_user_task_limit_message(count), also_delete=update.message)
            return

        animation_prompt_msg_id = session.get("animation_prompt_request_msg_id")
        for mid in filter(None, [update.message.message_id, animation_prompt_msg_id]):
            with contextlib.suppress(Exception):
                await update.message.get_bot().delete_message(update.message.chat_id, mid)
        return

    if state.get("state") == "WAITING_FIRST_LAST_PROMPT":
        prompt = update.message.text.strip()
        if not prompt:
            await reply_autodelete(update.message, "提示词不能为空，请重新输入或点击跳过：", also_delete=update.message)
            return

        error = validate_prompt_text(prompt)
        if error:
            warning_message = await update.message.reply_text(build_prompt_validation_reply(error))
            schedule_background_task(delete_message_later(update.message.get_bot(), update.message.chat_id, update.message.message_id, WARNING_DELETE_SECONDS))
            schedule_background_task(delete_message_later(warning_message.get_bot(), warning_message.chat_id, warning_message.message_id, WARNING_DELETE_SECONDS))
            return

        session_id = state.get("session_id")
        current_state = await pop_user_state_if_same(user_id, "WAITING_FIRST_LAST_PROMPT", session_id=session_id)
        if not current_state:
            return

        session = pending_prompt_sessions.get(session_id) if session_id else None
        if not session:
            await reply_autodelete(update.message, "⚠️ 图片已过期，请重新发图。", also_delete=update.message)
            return

        first_last_prompt_request_msg_id = session.pop("first_last_prompt_request_msg_id", None)
        for mid in filter(None, [update.message.message_id, first_last_prompt_request_msg_id]):
            with contextlib.suppress(Exception):
                await update.message.get_bot().delete_message(update.message.chat_id, mid)

        started = await _start_first_last_video(
            update.message, user_id, session_id, session, prompt, tg_user=update.message.from_user,
        )
        if not started:
            async with get_user_state_lock(user_id):
                if user_id not in user_states:
                    user_states[user_id] = current_state
        return

    if state.get("state") != "WAITING_CUSTOM_PROMPT":
        return  # 不在等待状态，忽略

    prompt = update.message.text.strip()
    if not prompt:
        await reply_autodelete(update.message, "提示词不能为空，请重新输入：", also_delete=update.message)
        return

    error = validate_prompt_text(prompt)
    if error:
        warning_message = await update.message.reply_text(build_prompt_validation_reply(error))
        schedule_background_task(delete_message_later(update.message.get_bot(), update.message.chat_id, update.message.message_id, WARNING_DELETE_SECONDS))
        schedule_background_task(delete_message_later(warning_message.get_bot(), warning_message.chat_id, warning_message.message_id, WARNING_DELETE_SECONDS))
        return

    session_id = state.get("session_id")
    if await is_user_task_limit_reached(user_id):
        await reply_autodelete(update.message, build_user_task_limit_message(), also_delete=update.message)
        return

    current_state = await pop_user_state_if_same(user_id, "WAITING_CUSTOM_PROMPT", session_id=session_id)
    if not current_state:
        return

    session = pending_prompt_sessions.get(session_id) if session_id else None
    if not session:
        await reply_autodelete(update.message, "⚠️ 图片已过期，请重新发图。", also_delete=update.message)
        return

    image_bytes = await get_session_image_bytes(session)
    if not image_bytes:
        await reply_autodelete(update.message, "⚠️ 会话已过期，请重新上传图片。", also_delete=update.message)
        return

    data = await load_data()
    user = get_user(data, user_id)
    api_key = user.get("api_key")

    scheduled, count = await schedule_user_processing_task(
        user_id,
        lambda: process_image(
            update.message,
            api_key,
            image_bytes,
            prompt,
            image_filename=session.get("image_filename", "input.jpg"),
            image_content_type=session.get("image_content_type", DEFAULT_IMAGE_CONTENT_TYPE),
        ),
    )
    if not scheduled:
        await reply_autodelete(update.message, build_user_task_limit_message(count), also_delete=update.message)
        return

    # 开始生成时立即删除引导消息和用户输入的提示词。
    prompt_request_msg_id = session.get("prompt_request_msg_id")
    for mid in filter(None, [update.message.message_id, prompt_request_msg_id]):
        with contextlib.suppress(Exception):
            await update.message.get_bot().delete_message(update.message.chat_id, mid)


async def cmd_aiprompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_aiprompt")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)

    if not context.args:
        current = user.get("ds_prompt")
        if current:
            await reply_autodelete(update.message, f"🤖 当前 AI 随机风格：\n\n{current}", also_delete=update.message)
        else:
            await reply_autodelete(update.message, "🤖 未设置，当前使用全局默认。\n用 /aiprompt <内容> 设置。", also_delete=update.message)
        return

    if context.args[0].lower() == "clear":
        def _mutate(data: dict):
            get_user(data, user_id).pop("ds_prompt", None)

        await update_data(_mutate)
        await reply_autodelete(update.message, "✅ 已清除，恢复使用全局默认提示词。", also_delete=update.message)
        return

    ds_prompt = " ".join(context.args).strip()
    def _mutate(data: dict):
        get_user(data, user_id)["ds_prompt"] = ds_prompt

    await update_data(_mutate)
    await reply_autodelete(update.message, "✅ 自定义 DS 提示词已保存。", also_delete=update.message)


async def cmd_gifsec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_gifsec")
    user_id = update.effective_user.id

    arg = context.args[0].strip() if context.args else ""
    if arg and arg not in {"5", "10"}:
        await reply_autodelete(update.message, "用法：/gifsec 或 /gifsec 5|10", also_delete=update.message)
        return

    def _mutate(data: dict):
        user = get_user(data, user_id)
        current = normalize_animation_seconds(user)
        next_seconds = int(arg) if arg else (10 if current == 5 else 5)
        user["animation_seconds"] = next_seconds
        return next_seconds

    seconds = await update_data(_mutate)
    await reply_autodelete(update.message, f"✅ 动图默认时长：{seconds}s", also_delete=update.message)


async def cmd_flprompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看 / 修改 / 重置首尾视频默认提示词。

    用法：
      /flprompt          —— 查看当前默认提示词
      /flprompt reset    —— 恢复系统默认提示词
      /flprompt <内容>   —— 设置自定义默认提示词
    """
    log_update_received(update, "cmd_flprompt")
    user_id = update.effective_user.id

    raw = (update.message.text or "").split(None, 1)
    arg = raw[1].strip() if len(raw) == 2 else ""

    if not arg:
        data = await load_data()
        user = get_user(data, user_id)
        custom = (user.get("first_last_prompt") or "").strip()
        prompt = custom or DEFAULT_FIRST_LAST_VIDEO_PROMPT
        source = "自定义" if custom else "系统默认"
        await update.message.reply_text(
            f"🎞️ 首尾视频默认提示词（{source}）：\n\n{prompt}\n\n"
            f"修改：/flprompt <新提示词>\n恢复系统默认：/flprompt reset"
        )
        return

    if arg.lower() == "reset":
        def _mutate_reset(data: dict):
            user = get_user(data, user_id)
            user.pop("first_last_prompt", None)
        await update_data(_mutate_reset)
        await reply_autodelete(update.message, "✅ 已恢复系统默认提示词。", also_delete=update.message)
        return

    error = validate_prompt_text(arg)
    if error:
        await reply_autodelete(update.message, f"❌ 不能保存：{error}", also_delete=update.message)
        return

    def _mutate_set(data: dict):
        user = get_user(data, user_id)
        user["first_last_prompt"] = arg
    await update_data(_mutate_set)
    await reply_autodelete(update.message, "✅ 首尾视频默认提示词已更新。", also_delete=update.message)


async def cmd_talkprompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看 / 修改 / 重置说话视频默认提示词。"""
    log_update_received(update, "cmd_talkprompt")
    user_id = update.effective_user.id

    raw = (update.message.text or "").split(None, 1)
    arg = raw[1].strip() if len(raw) == 2 else ""

    if not arg:
        data = await load_data()
        user = get_user(data, user_id)
        custom = (user.get("talking_video_prompt") or "").strip()
        prompt = custom or DEFAULT_TALKING_VIDEO_PROMPT
        source = "自定义" if custom else "系统默认"
        await update.message.reply_text(
            f"🗣️ 说话视频默认提示词（{source}）：\n\n{prompt}\n\n"
            f"修改：/talkprompt <新提示词>\n恢复系统默认：/talkprompt reset"
        )
        return

    if arg.lower() == "reset":
        def _mutate_reset(data: dict):
            user = get_user(data, user_id)
            user.pop("talking_video_prompt", None)
        await update_data(_mutate_reset)
        await reply_autodelete(update.message, "✅ 已恢复系统默认说话视频提示词。", also_delete=update.message)
        return

    error = validate_prompt_text(arg)
    if error:
        await reply_autodelete(update.message, f"❌ 不能保存：{error}", also_delete=update.message)
        return

    def _mutate_set(data: dict):
        user = get_user(data, user_id)
        user["talking_video_prompt"] = arg
    await update_data(_mutate_set)
    await reply_autodelete(update.message, "✅ 说话视频默认提示词已更新。", also_delete=update.message)


async def cmd_comparetext(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """第一步：引导用户输入原图文案（最多6字）。"""
    log_update_received(update, "cmd_comparetext")
    user_id = update.effective_user.id
    chat_id = update.message.chat_id
    flow_id = uuid.uuid4().hex
    success, current_state, already_same = await claim_user_state(
        user_id,
        {"state": "WAITING_COMPARE_ORIG", "flow_id": flow_id},
        "WAITING_COMPARE_ORIG",
    )
    if not success:
        await reply_autodelete(update.message, build_waiting_state_conflict_message(current_state), also_delete=update.message)
        return
    if already_same:
        # 用户已经在流程中，显示当前文案状态并重新开始
        user_states.pop(user_id, None)
        # 重新走一次流程
        data = await load_data()
        user = get_user(data, user_id)
        orig = user.get("compare_origin_text", "")
        result = user.get("compare_result_text", "")
        info = "📝 当前文案：\n左边（原图）：" + (orig or "（未设置）") + "\n右边（编辑后）：" + (result or "（未设置）")
        info_msg = await update.message.reply_text(info)
        schedule_background_task(delete_message_later(info_msg.get_bot(), info_msg.chat_id, info_msg.message_id, WARNING_DELETE_SECONDS))
        # 重新开始
        flow_id = uuid.uuid4().hex
        success2, _, _ = await claim_user_state(
            user_id,
            {"state": "WAITING_COMPARE_ORIG", "flow_id": flow_id},
            "WAITING_COMPARE_ORIG",
        )
        if not success2:
            await reply_autodelete(update.message, "请稍后重试 /comparetext。", also_delete=update.message)
            return
    prompt_msg = await update.message.reply_text("🪞 请输入对比图左边的原图文案（最多6字）：")
    # 把 bot 引导消息 ID 和命令消息 ID 存到 state 里，完成后立即删除
    cmd_msg_id = update.message.message_id
    async with get_user_state_lock(user_id):
        state = user_states.get(user_id)
        if state and state.get("flow_id") == flow_id:
            state["_prompt_msg_id"] = prompt_msg.message_id
            state["_cmd_msg_id"] = cmd_msg_id
            state["_chat_id"] = chat_id
    schedule_background_task(delete_message_later(update.message.get_bot(), chat_id, cmd_msg_id, WAITING_FLOW_TIMEOUT_SECONDS))
    schedule_background_task(delete_message_later(prompt_msg.get_bot(), chat_id, prompt_msg.message_id, WAITING_FLOW_TIMEOUT_SECONDS))
    schedule_background_task(expire_waiting_flow_later(
        user_id, "WAITING_COMPARE_ORIG", "flow_id", flow_id,
        prompt_msg.get_bot(), chat_id, prompt_msg.message_id,
        flow_id=flow_id,
    ))


async def cmd_compareswitch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送对比图开关面板。"""
    log_update_received(update, "cmd_compareswitch")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)
    switches = user.get("compare_switches", {})
    def _label(key, name):
        return f"{'✅' if switches.get(key) else '❌'} {name}"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(_label("faceswap", "参考图换脸"), callback_data="compare:faceswap")],
        [InlineKeyboardButton(_label("bodyswap", "参考换衣"), callback_data="compare:bodyswap")],
        [InlineKeyboardButton(_label("custom", "自定义提示"), callback_data="compare:custom")],
    ])
    reply_msg = await update.message.reply_text("🔧 对比图开关（点击切换）：", reply_markup=keyboard)
    bot = update.message.get_bot()
    chat_id = update.message.chat_id
    schedule_background_task(delete_message_later(bot, chat_id, update.message.message_id, WAITING_FLOW_TIMEOUT_SECONDS))
    schedule_background_task(delete_message_later(bot, chat_id, reply_msg.message_id, WAITING_FLOW_TIMEOUT_SECONDS))


async def cmd_expand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """发送图片扩展方向开关面板。"""
    log_update_received(update, "cmd_expand")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)
    reply_msg = await update.message.reply_text(
        "🖼️ 图片扩展方向（开=200px，关=0px）：",
        reply_markup=build_image_extend_keyboard(user),
    )
    bot = update.message.get_bot()
    chat_id = update.message.chat_id
    schedule_background_task(delete_message_later(bot, chat_id, update.message.message_id, WAITING_FLOW_TIMEOUT_SECONDS))
    schedule_background_task(delete_message_later(bot, chat_id, reply_msg.message_id, WAITING_FLOW_TIMEOUT_SECONDS))


async def cmd_presetflow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """查看/切换参考换衣工作流（红火 / 千问）。"""
    log_update_received(update, "cmd_presetflow")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)
    current = user.get("preset_workflow", DEFAULT_PRESET_WORKFLOW_KEY)
    label = "红火" if current == "firered" else "千问"
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("👙 红火", callback_data="presetflow:firered"),
        InlineKeyboardButton("👗 千问", callback_data="presetflow:qwen"),
    ]])
    reply_msg = await update.message.reply_text(
        f"🔧 参考换衣工作流\n当前：{label}\n\n选择要切换的工作流：",
        reply_markup=keyboard,
    )
    bot = update.message.get_bot()
    chat_id = update.message.chat_id
    # 记录命令消息 ID，供回调删除；5 分钟兜底双删
    presetflow_pending[user_id] = {"cmd_msg_id": update.message.message_id, "chat_id": chat_id}
    schedule_background_task(delete_message_later(bot, chat_id, update.message.message_id, WAITING_FLOW_TIMEOUT_SECONDS))
    schedule_background_task(delete_message_later(bot, chat_id, reply_msg.message_id, WAITING_FLOW_TIMEOUT_SECONDS))


async def cmd_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_reset")
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("确定", callback_data="reset:confirm"),
        InlineKeyboardButton("取消", callback_data="reset:cancel"),
    ]])
    await update.message.reply_text("⚠️ 确定要清空所有设置吗？", reply_markup=keyboard)


async def cmd_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    log_update_received(update, "cmd_key")
    user_id = update.effective_user.id
    data = await load_data()
    user = get_user(data, user_id)

    if context.args:
        api_key = context.args[0].strip()
        # 校验格式：长度在合理区间、仅允许可见 ASCII 字符
        if not (16 <= len(api_key) <= 128) or not re.fullmatch(r"[A-Za-z0-9_\-]+", api_key):
            await reply_autodelete(
                update.message,
                "❌ API Key 格式不对，应为 16-128 位英文/数字/下划线/短横线。\n"
                "请用 /key <你的 RunningHub API Key> 重新设置。",
                also_delete=update.message,
            )
            return

        def _mutate(data: dict):
            get_user(data, user_id)["api_key"] = api_key

        await update_data(_mutate)
        await reply_autodelete(update.message, "✅ API Key 已更新！现在可以发图开始处理了。", also_delete=update.message)
        return

    api_key = user.get("api_key")
    if api_key:
        await reply_autodelete(update.message,
            f"🔑 当前 Key：{mask_api_key(api_key)}\n用 /key <新 Key> 更换。",
            also_delete=update.message)
    else:
        await reply_autodelete(update.message,
            "🔑 未设置 API Key。\n用 /key <RunningHub API Key> 设置。",
            also_delete=update.message)


# ─── 主入口 ──────────────────────────────────

def main():
    validate_runtime_config()
    logger.info("Bot 启动中……")
    migrate_result_archive_dir()
    cleanup_runtime_temp_dirs()

    async def _run():
        """在同一个事件循环内构建 App，只重启轮询连接，后台任务不受影响。"""
        async def on_startup(application):
            await open_shared_http_session()
            with contextlib.suppress(Exception):
                await application.bot.set_my_commands(build_bot_commands())
            try:
                bot_info = await application.bot.get_me()
                username = f"@{bot_info.username}"
            except Exception:
                username = "@unknown"
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("\n" + "━" * 44)
            print(f"  ✅  Bot 启动成功！{username}")
            print(f"  🕐  {now}")
            print("━" * 44 + "\n")

        async def on_stop(application):
            """app.stop() 时：取消所有后台任务。"""
            await cancel_active_tasks()

        async def on_shutdown(application):
            """app.shutdown() 时：释放底层资源。"""
            await close_shared_http_session()

        # 在事件循环内构建 App，避免 asyncio 对象绑定到不同 loop
        app = (
            ApplicationBuilder()
            .token(TG_TOKEN)
            .concurrent_updates(32)
            .connection_pool_size(TG_CONNECTION_POOL_SIZE)
            .pool_timeout(TG_POOL_TIMEOUT)
            .connect_timeout(TG_CONNECT_TIMEOUT)
            .read_timeout(TG_READ_TIMEOUT)
            .write_timeout(TG_WRITE_TIMEOUT)
            .media_write_timeout(TG_WRITE_TIMEOUT)
            .get_updates_connection_pool_size(TG_GET_UPDATES_POOL_SIZE)
            .get_updates_pool_timeout(TG_POOL_TIMEOUT)
            .get_updates_connect_timeout(TG_CONNECT_TIMEOUT)
            .get_updates_read_timeout(TG_GET_UPDATES_READ_TIMEOUT)
            .get_updates_write_timeout(TG_WRITE_TIMEOUT)
            .post_init(on_startup)
            .post_stop(on_stop)
            .post_shutdown(on_shutdown)
            .build()
        )

        app.add_handler(CommandHandler("start",      cmd_start))
        app.add_handler(CommandHandler("key",        cmd_key))
        app.add_handler(CommandHandler("aiprompt",   cmd_aiprompt))
        app.add_handler(CommandHandler("gifsec",     cmd_gifsec))
        app.add_handler(CommandHandler("flprompt",   cmd_flprompt))
        app.add_handler(CommandHandler("talkprompt", cmd_talkprompt))
        app.add_handler(CommandHandler("save",       cmd_saveprompt))
        app.add_handler(CommandHandler("saveimg",    cmd_saveimg))
        app.add_handler(CommandHandler("savevoice",  cmd_savevoice))
        app.add_handler(CommandHandler("voice",      cmd_voice))
        app.add_handler(CommandHandler("del",        cmd_delprompt))
        app.add_handler(CommandHandler("expand",     cmd_expand))
        app.add_handler(CommandHandler("presetflow",    cmd_presetflow))
        app.add_handler(CommandHandler("reset",         cmd_reset))
        app.add_handler(CommandHandler("comparetext",   cmd_comparetext))
        app.add_handler(CommandHandler("compareswitch", cmd_compareswitch))
        app.add_handler(MessageHandler(filters.PHOTO,          handle_photo))
        app.add_handler(MessageHandler(filters.VOICE,          handle_voice_message))
        app.add_handler(MessageHandler(filters.AUDIO,          handle_audio_message))
        app.add_handler(MessageHandler(filters.Document.AUDIO, handle_audio_document))
        app.add_handler(MessageHandler(filters.VIDEO,          handle_video))
        app.add_handler(MessageHandler(filters.Document.VIDEO, handle_video_document))
        app.add_handler(MessageHandler(filters.Document.IMAGE, handle_document))
        app.add_handler(CallbackQueryHandler(handle_callback))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
        app.add_error_handler(handle_application_error)

        cst = timezone(timedelta(hours=8))
        app.job_queue.run_daily(midnight_cleanup_job, time=dt_time(0, 0, 0, tzinfo=cst))

        await app.initialize()  # 触发 on_startup
        await app.start()       # 启动 JobQueue 等

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(Exception):
                loop.add_signal_handler(sig, stop_event.set)

        try:
            while not stop_event.is_set():
                try:
                    await app.updater.start_polling(
                        drop_pending_updates=False,
                        bootstrap_retries=-1,
                    )
                    logger.info("Telegram 轮询已连接，等待消息……")
                    # 监控轮询任务是否意外停止（Conflict 会走 error handler → os._exit，
                    # 其余异常会令 updater.running 变 False）。
                    # 同时检测系统休眠唤醒：sleep(5) 实际过了远超 5 秒，
                    # 说明系统刚被挂起，long-poll 大概率已是僵尸连接，主动重连。
                    last_tick = time.time()
                    suspended = False
                    while not stop_event.is_set():
                        if not app.updater.running:
                            break
                        await asyncio.sleep(5)
                        now = time.time()
                        # 系统休眠唤醒检测：sleep(5) 实际过了远超 60 秒
                        if now - last_tick > 60:
                            logger.warning(
                                "检测到时间跳跃 %.0f 秒（疑似系统休眠唤醒），主动重连轮询……",
                                now - last_tick,
                            )
                            suspended = True
                            break
                        last_tick = now
                    if stop_event.is_set():
                        break
                    if not suspended:
                        logger.warning("轮询意外停止，5 秒后重连……")
                    with contextlib.suppress(Exception):
                        await app.updater.stop()
                    # 唤醒后共享 HTTP Session 里的 keep-alive 连接基本已失效，
                    # 关掉让下次请求重建，避免首批请求超时。
                    if suspended:
                        await close_shared_http_session()
                        await open_shared_http_session()
                    await asyncio.sleep(5)
                except Exception as exc:
                    logger.warning("Telegram 连接中断：%s，5 秒后重连……", exc)
                    with contextlib.suppress(Exception):
                        await app.updater.stop()
                    if not stop_event.is_set():
                        await asyncio.sleep(5)
        finally:
            with contextlib.suppress(Exception):
                await app.updater.stop()
            await app.stop()
            await app.shutdown()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
