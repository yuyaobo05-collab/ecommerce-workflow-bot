import io
import math
import struct
import sys
from pathlib import Path

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from duck_decode import decode_duck_image, decode_duck_media


def _make_binary_png(payload: bytes, width: int = 4) -> bytes:
    height = max(1, math.ceil(len(payload) / (width * 3)))
    padded = np.zeros(height * width * 3, dtype=np.uint8)
    padded[:len(payload)] = np.frombuffer(payload, dtype=np.uint8)
    image = Image.fromarray(padded.reshape(height, width, 3))
    out = io.BytesIO()
    image.save(out, format="PNG")
    return out.getvalue()


def _make_duck_carrier(payload: bytes, ext: str, width: int = 80, height: int = 40) -> bytes:
    ext_bytes = ext.encode("utf-8")
    header = b"\x00" + bytes([len(ext_bytes)]) + ext_bytes + struct.pack(">I", len(payload)) + payload
    packet = struct.pack(">I", len(header)) + header

    arr = np.zeros((height, width, 3), dtype=np.uint8)
    skip_w = int(width * 0.40)
    skip_h = int(height * 0.08)
    mask2d = np.ones((height, width), dtype=bool)
    mask2d[:skip_h, :skip_w] = False
    mask3d = np.repeat(mask2d[:, :, None], 3, axis=2)
    idxs = np.flatnonzero(mask3d.reshape(-1))
    assert len(packet) <= len(idxs)

    flat = arr.reshape(-1)
    flat[idxs[:len(packet)]] = np.frombuffer(packet, dtype=np.uint8)

    out = io.BytesIO()
    Image.fromarray(arr).save(out, format="PNG")
    return out.getvalue()


def test_decode_duck_media_restores_binpng_video_payload():
    video_bytes = b"\x00\x00\x00\x18ftypmp42example-video-bytes"
    binpng_bytes = _make_binary_png(video_bytes)
    carrier = _make_duck_carrier(binpng_bytes, "mp4.binpng")

    assert decode_duck_media(carrier) == (video_bytes, "mp4")
    assert decode_duck_image(carrier) is None
