import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from runninghub import build_node_info_list
from workflows.registry import WORKFLOWS


def test_custom_default_node_info_maps_image_and_prompt():
    spec = WORKFLOWS["custom_default"]
    nodes = build_node_info_list(
        spec,
        {
            "image": "input.png",
            "prompt": "make it cinematic",
        },
    )

    assert nodes == [
        {"nodeId": "16", "fieldName": "image", "fieldValue": "input.png"},
        {"nodeId": "5", "fieldName": "prompt", "fieldValue": "make it cinematic"},
    ]


def test_outfit_extract_adds_fixed_prompt():
    spec = WORKFLOWS["outfit_extract"]
    nodes = build_node_info_list(spec, {"image": "cloth.png"})

    assert nodes == [
        {"nodeId": "10", "fieldName": "image", "fieldValue": "cloth.png"},
        {
            "nodeId": "16",
            "fieldName": "text",
            "fieldValue": "Extract the clothes from the character and display them flat",
        },
    ]


def test_image_expand_node_info_maps_all_sides():
    spec = WORKFLOWS["image_expand"]
    nodes = build_node_info_list(
        spec,
        {
            "image": "input.png",
            "top": 200,
            "bottom": 0,
            "right": 200,
            "left": 0,
        },
    )

    assert nodes == [
        {"nodeId": "166", "fieldName": "image", "fieldValue": "input.png"},
        {"nodeId": "162", "fieldName": "top", "fieldValue": "200"},
        {"nodeId": "162", "fieldName": "bottom", "fieldValue": "0"},
        {"nodeId": "162", "fieldName": "right", "fieldValue": "200"},
        {"nodeId": "162", "fieldName": "left", "fieldValue": "0"},
    ]


def test_image_animation_node_info_maps_prompt_and_time():
    spec = WORKFLOWS["image_animation"]
    nodes = build_node_info_list(
        spec,
        {
            "image": "input.png",
            "prompt": "wave to camera",
            "time": 5,
        },
    )

    assert nodes == [
        {"nodeId": "37", "fieldName": "image", "fieldValue": "input.png"},
        {"nodeId": "106", "fieldName": "value", "fieldValue": "wave to camera"},
        {"nodeId": "104", "fieldName": "value", "fieldValue": "5"},
    ]


def test_image_animation_node_info_keeps_empty_prompt_node():
    spec = WORKFLOWS["image_animation"]
    nodes = build_node_info_list(spec, {"image": "input.png", "time": 5})

    assert nodes == [
        {"nodeId": "37", "fieldName": "image", "fieldValue": "input.png"},
        {"nodeId": "106", "fieldName": "value", "fieldValue": ""},
        {"nodeId": "104", "fieldName": "value", "fieldValue": "5"},
    ]


def test_video_outfit_node_info_maps_video_and_reference_image():
    spec = WORKFLOWS["video_outfit"]
    nodes = build_node_info_list(
        spec,
        {
            "video": "input.mp4",
            "reference_image": "cloth.jpg",
        },
    )

    assert nodes == [
        {"nodeId": "114", "fieldName": "video", "fieldValue": "input.mp4"},
        {"nodeId": "339", "fieldName": "image", "fieldValue": "cloth.jpg"},
    ]


def test_first_last_video_node_info_maps_first_last_max_side_time_and_prompt():
    spec = WORKFLOWS["first_last_video"]
    nodes = build_node_info_list(
        spec,
        {
            "first_image": "first.png",
            "last_image": "last.png",
            "max_side": 1024,
            "time": 5,
            "prompt": "make it cinematic",
        },
    )

    assert nodes == [
        {"nodeId": "110", "fieldName": "image", "fieldValue": "first.png"},
        {"nodeId": "111", "fieldName": "image", "fieldValue": "last.png"},
        {"nodeId": "104", "fieldName": "value", "fieldValue": "1024"},
        {"nodeId": "101", "fieldName": "value", "fieldValue": "5"},
        {"nodeId": "131", "fieldName": "text", "fieldValue": "make it cinematic"},
    ]


def test_scene_replace_node_info_maps_scene_and_person_images():
    spec = WORKFLOWS["scene_replace"]
    nodes = build_node_info_list(
        spec,
        {
            "scene_image": "scene.jpg",
            "person_image": "person.jpg",
        },
    )

    assert nodes == [
        {"nodeId": "17", "fieldName": "image", "fieldValue": "scene.jpg"},
        {"nodeId": "44", "fieldName": "image", "fieldValue": "person.jpg"},
    ]


def test_talking_video_node_info_maps_image_audio_time_and_prompt():
    spec = WORKFLOWS["talking_video"]
    nodes = build_node_info_list(
        spec,
        {
            "image": "portrait.jpg",
            "audio": "speech.flac",
            "time": 10,
            "prompt": "subtle lip sync",
        },
    )

    assert nodes == [
        {"nodeId": "444", "fieldName": "image", "fieldValue": "portrait.jpg"},
        {"nodeId": "1594", "fieldName": "audio", "fieldValue": "speech.flac"},
        {"nodeId": "1583", "fieldName": "value", "fieldValue": "10"},
        {"nodeId": "1624", "fieldName": "value", "fieldValue": "subtle lip sync"},
    ]


def test_voice_clone_node_info_maps_audio_and_text():
    spec = WORKFLOWS["voice_clone"]
    nodes = build_node_info_list(
        spec,
        {
            "sample_audio": "sample.ogg",
            "text": "hello world",
        },
    )

    assert nodes == [
        {"nodeId": "33", "fieldName": "audio", "fieldValue": "sample.ogg"},
        {"nodeId": "36", "fieldName": "value", "fieldValue": "hello world"},
    ]
