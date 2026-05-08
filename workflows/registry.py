from dataclasses import dataclass, field
import os
from typing import Mapping


NodeRef = tuple[str, str]


@dataclass(frozen=True)
class WorkflowSpec:
    key: str
    label: str
    endpoint: str
    input_mode: str
    node_order: tuple[str, ...]
    nodes: Mapping[str, NodeRef]
    fixed_values: Mapping[str, str] = field(default_factory=dict)


DEFAULT_CUSTOM_WORKFLOW_KEY = "custom_default"
DEFAULT_PRESET_WORKFLOW_KEY = "firered"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _runninghub_ai_app_endpoint(value: str) -> str:
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.isdigit():
        return f"https://www.runninghub.cn/openapi/v2/run/ai-app/{value}"
    return value


WORKFLOWS: dict[str, WorkflowSpec] = {
    "custom_default": WorkflowSpec(
        key="custom_default",
        label="默认自定义",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2050993778437177345",
        input_mode="image_prompt",
        node_order=("image", "prompt"),
        nodes={
            "image": ("16", "image"),
            "prompt": ("5", "prompt"),
        },
    ),
    "faceswap": WorkflowSpec(
        key="faceswap",
        label="参考换脸",
        endpoint="https://www.runninghub.cn/openapi/v2/run/workflow/2046609475062210561",
        input_mode="image_image",
        node_order=("original_image", "face_image"),
        nodes={
            "original_image": ("91", "image"),
            "face_image": ("20", "image"),
        },
    ),
    "outfit_extract": WorkflowSpec(
        key="outfit_extract",
        label="提取装扮",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2047128177683734529",
        input_mode="image_fixed_prompt",
        node_order=("image", "prompt"),
        nodes={
            "image": ("10", "image"),
            "prompt": ("16", "text"),
        },
        fixed_values={
            "prompt": "Extract the clothes from the character and display them flat",
        },
    ),
    "firered": WorkflowSpec(
        key="firered",
        label="红火换装",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2046962002597257217",
        input_mode="image_reference",
        node_order=("original_image", "reference_image"),
        nodes={
            "original_image": ("207", "image"),
            "reference_image": ("208", "image"),
        },
    ),
    "qwen": WorkflowSpec(
        key="qwen",
        label="千问换装",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2047721143859159041",
        input_mode="image_reference",
        node_order=("original_image", "reference_image"),
        nodes={
            "original_image": ("7", "image"),
            "reference_image": ("89", "image"),
        },
    ),
    "image_expand": WorkflowSpec(
        key="image_expand",
        label="图片扩展",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2050497210290327554",
        input_mode="image_expand",
        node_order=("image", "top", "bottom", "right", "left"),
        nodes={
            "image": ("166", "image"),
            "top": ("162", "top"),
            "bottom": ("162", "bottom"),
            "right": ("162", "right"),
            "left": ("162", "left"),
        },
    ),
    "image_animation": WorkflowSpec(
        key="image_animation",
        label="生成动图",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2050956192196898818",
        input_mode="image_prompt_time",
        node_order=("image", "prompt", "time"),
        nodes={
            "image": ("37", "image"),
            "prompt": ("106", "value"),
            "time": ("104", "value"),
        },
        fixed_values={
            "prompt": "",
        },
    ),
    "video_outfit": WorkflowSpec(
        key="video_outfit",
        label="视频换衣",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2050968116758364161",
        input_mode="video_reference",
        node_order=("video", "reference_image"),
        nodes={
            "video": ("114", "video"),
            "reference_image": ("339", "image"),
        },
    ),
    "first_last_video": WorkflowSpec(
        key="first_last_video",
        label="首尾视频",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2051019619913216002",
        input_mode="image_image_size_time_prompt",
        node_order=("first_image", "last_image", "max_side", "time", "prompt"),
        nodes={
            "first_image": ("110", "image"),
            "last_image": ("111", "image"),
            "max_side": ("104", "value"),
            "time": ("101", "value"),
            "prompt": ("131", "text"),
        },
    ),
    "scene_replace": WorkflowSpec(
        key="scene_replace",
        label="场景换人",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2051245294918090754",
        input_mode="image_image",
        node_order=("scene_image", "person_image"),
        nodes={
            "scene_image": ("17", "image"),
            "person_image": ("44", "image"),
        },
    ),
    "talking_video": WorkflowSpec(
        key="talking_video",
        label="说话视频",
        endpoint="https://www.runninghub.cn/openapi/v2/run/ai-app/2051306076557070338",
        input_mode="image_audio_time_prompt",
        node_order=("image", "audio", "time", "prompt"),
        nodes={
            "image": ("444", "image"),
            "audio": ("1594", "audio"),
            "time": ("1583", "value"),
            "prompt": ("1624", "value"),
        },
    ),
    "voice_clone": WorkflowSpec(
        key="voice_clone",
        label="声音克隆",
        endpoint=_runninghub_ai_app_endpoint(
            _env("RH_VOICE_CLONE_ENDPOINT", "2051223049248227329")
        ),
        input_mode="audio_text",
        node_order=("sample_audio", "text"),
        nodes={
            "sample_audio": (
                _env("RH_VOICE_SAMPLE_NODE_ID", "33"),
                _env("RH_VOICE_SAMPLE_FIELD", "audio"),
            ),
            "text": (
                _env("RH_VOICE_TEXT_NODE_ID", "36"),
                _env("RH_VOICE_TEXT_FIELD", "value"),
            ),
        },
    ),
}


CUSTOM_WORKFLOW_KEYS = ("custom_default",)
PRESET_WORKFLOW_KEYS = ("firered", "qwen")
