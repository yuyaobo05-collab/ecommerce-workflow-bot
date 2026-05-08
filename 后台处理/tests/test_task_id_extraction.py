import pytest
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bot import _load_env_file, extract_task_id_or_raise, validate_runtime_config


def test_extract_task_id_from_nested_data():
    data = {
        "code": 0,
        "msg": "success",
        "data": {
            "taskId": "1234567890",
            "status": "RUNNING",
        },
    }

    assert extract_task_id_or_raise(data, "工作流") == "1234567890"


def test_extract_task_id_raises_useful_error_when_missing():
    data = {
        "code": 812,
        "msg": "Insufficient corporate funds, please top up | 企业版余额不足，请充值",
        "data": {
            "taskId": "",
            "status": "",
            "errorCode": "812",
            "errorMessage": "Insufficient corporate funds, please top up | 企业版余额不足，请充值",
        },
    }

    with pytest.raises(RuntimeError) as exc_info:
        extract_task_id_or_raise(data, "工作流")

    message = str(exc_info.value)
    assert "errorCode=812" in message
    assert "企业版余额不足" in message


def test_extract_task_id_uses_top_level_error_when_data_is_empty():
    data = {
        "code": 812,
        "msg": "Insufficient corporate funds, please top up | 企业版余额不足，请充值",
        "data": {},
    }

    with pytest.raises(RuntimeError) as exc_info:
        extract_task_id_or_raise(data, "工作流")

    message = str(exc_info.value)
    assert "errorCode=812" in message
    assert "企业版余额不足" in message


def test_import_does_not_require_runtime_secrets(monkeypatch):
    import bot

    monkeypatch.setattr(bot, "TG_TOKEN", "")
    monkeypatch.setattr(bot, "DS_API_KEY", "")

    with pytest.raises(RuntimeError) as exc_info:
        validate_runtime_config()

    assert "TG_TOKEN" in str(exc_info.value)


def test_load_env_file_sets_missing_values(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# comment",
                "TG_TOKEN=123456:abc",
                "DS_API_KEY='sk-test'",
                "export RH_VOICE_CLONE_ENDPOINT=2051223049248227329",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("TG_TOKEN", raising=False)
    monkeypatch.delenv("DS_API_KEY", raising=False)
    monkeypatch.delenv("RH_VOICE_CLONE_ENDPOINT", raising=False)

    _load_env_file(env_file)

    assert os.environ["TG_TOKEN"] == "123456:abc"
    assert os.environ["DS_API_KEY"] == "sk-test"
    assert os.environ["RH_VOICE_CLONE_ENDPOINT"] == "2051223049248227329"


def test_load_env_file_does_not_override_existing_env(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("TG_TOKEN=from-file\n", encoding="utf-8")
    monkeypatch.setenv("TG_TOKEN", "from-env")

    _load_env_file(env_file)

    assert os.environ["TG_TOKEN"] == "from-env"
