"""
鸭子图 LSB 隐写解码 — 纯 Python 实现
来源：https://github.com/copyangle/SS_tools（duck_decode_node.py）

直接在进程内运行，无需子进程，无冷启动开销。
"""

import io
import struct
import hashlib
from typing import Optional
import numpy as np
from PIL import Image

WATERMARK_SKIP_W_RATIO = 0.40
WATERMARK_SKIP_H_RATIO = 0.08
IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp"}
VIDEO_EXTENSIONS = {"mp4", "mov", "webm", "avi", "mkv"}


def _extract_payload_with_k(arr: np.ndarray, k: int) -> bytes:
    h, w, c = arr.shape
    skip_w = int(w * WATERMARK_SKIP_W_RATIO)
    skip_h = int(h * WATERMARK_SKIP_H_RATIO)
    mask2d = np.ones((h, w), dtype=bool)
    if skip_w > 0 and skip_h > 0:
        mask2d[:skip_h, :skip_w] = False
    mask3d = np.repeat(mask2d[:, :, None], c, axis=2)
    flat = arr.reshape(-1)
    idxs = np.flatnonzero(mask3d.reshape(-1))
    vals = (flat[idxs] & ((1 << k) - 1)).astype(np.uint8)
    ub = np.unpackbits(vals, bitorder="big").reshape(-1, 8)[:, -k:]
    bits = ub.reshape(-1)
    if len(bits) < 32:
        raise ValueError("Insufficient image data")
    length_bytes = np.packbits(bits[:32], bitorder="big").tobytes()
    header_len = struct.unpack(">I", length_bytes)[0]
    total_bits = 32 + header_len * 8
    if header_len <= 0 or total_bits > len(bits):
        raise ValueError("Payload length invalid")
    payload_bits = bits[32:32 + header_len * 8]
    return np.packbits(payload_bits, bitorder="big").tobytes()


def _generate_key_stream(password: str, salt: bytes, length: int) -> bytes:
    key_material = (password + salt.hex()).encode("utf-8")
    out = bytearray()
    counter = 0
    while len(out) < length:
        out.extend(hashlib.sha256(key_material + str(counter).encode()).digest())
        counter += 1
    return bytes(out[:length])


def _parse_header(header: bytes, password: str = "") -> tuple[bytes, str]:
    idx = 0
    if len(header) < 1:
        raise ValueError("Header corrupted")
    has_pwd = header[0] == 1
    idx += 1
    pwd_hash = salt = b""
    if has_pwd:
        if len(header) < idx + 48:
            raise ValueError("Header corrupted")
        pwd_hash = header[idx:idx + 32]; idx += 32
        salt     = header[idx:idx + 16]; idx += 16
    if len(header) < idx + 1:
        raise ValueError("Header corrupted")
    ext_len = header[idx]; idx += 1
    if len(header) < idx + ext_len + 4:
        raise ValueError("Header corrupted")
    ext      = header[idx:idx + ext_len].decode("utf-8", errors="ignore"); idx += ext_len
    data_len = struct.unpack(">I", header[idx:idx + 4])[0]; idx += 4
    data     = header[idx:]
    if len(data) != data_len:
        raise ValueError("Data length mismatch")
    if not has_pwd:
        return data, ext
    if not password:
        raise ValueError("Password required")
    check_hash = hashlib.sha256((password + salt.hex()).encode()).digest()
    if check_hash != pwd_hash:
        raise ValueError("Wrong password")
    ks = _generate_key_stream(password, salt, len(data))
    return bytes(a ^ b for a, b in zip(data, ks)), ext


def _decode_duck_payload(image_bytes: bytes, password: str = "") -> Optional[tuple[bytes, str]]:
    """
    解码鸭子图，返回隐藏载荷 bytes 和扩展名，失败返回 None。
    """
    try:
        pil = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        arr = np.array(pil, dtype=np.uint8)
        for k in (2, 6, 8):
            try:
                header   = _extract_payload_with_k(arr, k)
                raw, ext = _parse_header(header, password)
                return raw, ext.lower().lstrip(".")
            except Exception:
                continue
        return None
    except Exception:
        return None


def _binpng_bytes_to_file_bytes(binpng_bytes: bytes) -> bytes:
    pil = Image.open(io.BytesIO(binpng_bytes)).convert("RGB")
    arr = np.array(pil, dtype=np.uint8)
    return arr.reshape(-1).tobytes().rstrip(b"\x00")


def decode_duck_media(image_bytes: bytes, password: str = "") -> Optional[tuple[bytes, str]]:
    """
    解码鸭子图里的图片或视频载荷。

    视频工作流会把视频 bytes 先转成 PNG 载荷，扩展名形如 mp4.binpng；
    这里按 SS_tools 的方式把该 PNG 像素重新铺平成原始视频 bytes。
    """
    try:
        decoded = _decode_duck_payload(image_bytes, password)
        if not decoded:
            return None
        raw, ext = decoded
        if ext in IMAGE_EXTENSIONS or ext in VIDEO_EXTENSIONS:
            return raw, ext
        if ext.endswith(".binpng"):
            base_ext = ext[:-len(".binpng")].rsplit(".", 1)[-1] or "mp4"
            return _binpng_bytes_to_file_bytes(raw), base_ext
        return None
    except Exception:
        return None


def decode_duck_image(image_bytes: bytes, password: str = "") -> Optional[bytes]:
    """
    解码鸭子图，返回提取出的图片 bytes（PNG/JPG），失败或非图片载荷返回 None。
    纯内存操作，不写临时文件。
    """
    decoded = _decode_duck_payload(image_bytes, password)
    if not decoded:
        return None
    raw, ext = decoded
    if ext in IMAGE_EXTENSIONS:
        return raw
    return None
