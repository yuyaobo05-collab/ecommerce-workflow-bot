import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bot import DS_PROMPT_MAX_CHARS, _extract_ds_prompt_from_response


def _response(content, finish_reason="stop"):
    return {
        "choices": [
            {
                "finish_reason": finish_reason,
                "message": {"content": content},
            }
        ]
    }


def test_extract_ds_prompt_accepts_complete_prompt():
    prompt = "禁止调整角色面部特征，保持一致，在柔和自然光下站在花园里，镜头正面构图。"

    assert _extract_ds_prompt_from_response(_response(prompt)) == prompt


def test_extract_ds_prompt_normalizes_label_and_code_fence():
    prompt = "禁止调整角色面部特征，保持一致，在黄昏街道中散步，镜头稳定构图。"
    data = _response(f"```text\n提示词：{prompt}\n```")

    assert _extract_ds_prompt_from_response(data) == prompt


def test_extract_ds_prompt_rejects_empty_content():
    with pytest.raises(RuntimeError, match="空内容|过短"):
        _extract_ds_prompt_from_response(_response(""))


def test_extract_ds_prompt_rejects_length_finish_reason():
    prompt = "禁止调整角色面部特征，保持一致，在柔和自然光下站在花园里，镜头正面构图。"

    with pytest.raises(RuntimeError, match="截断"):
        _extract_ds_prompt_from_response(_response(prompt, finish_reason="length"))


def test_extract_ds_prompt_rejects_incomplete_tail():
    prompt = "禁止调整角色面部特征，保持一致，在柔和自然光下站在花园里，镜头正面构图，"

    with pytest.raises(RuntimeError, match="未完整收尾"):
        _extract_ds_prompt_from_response(_response(prompt))


def test_extract_ds_prompt_rejects_overly_long_content():
    prompt = "很" * DS_PROMPT_MAX_CHARS + "。"

    with pytest.raises(RuntimeError, match="过长"):
        _extract_ds_prompt_from_response(_response(prompt))
