#!/usr/bin/env python3
"""
captcha_solver.py — ddddocr-based captcha solver for invoice-verifier.

Supports three captcha types based on key4:
  - key4=01 (red_only):  color-filter red pixels → ddddocr
  - key4=03 (blue_only): color-filter blue pixels → ddddocr
  - key4=00/02 (all_chars): direct ddddocr

Usage:
    from captcha_solver import solve_captcha
    result = solve_captcha(img_base64_str, key4)
"""

from __future__ import annotations

import base64
import io
from typing import Optional

import ddddocr
import numpy as np
from PIL import Image

# Global singleton — avoid re-initializing the model on every call
_ocr_instance: Optional[ddddocr.DdddOcr] = None


def _get_ocr() -> ddddocr.DdddOcr:
    global _ocr_instance
    if _ocr_instance is None:
        _ocr_instance = ddddocr.DdddOcr(show_ad=False, beta=True)
    return _ocr_instance


def _color_filter(img_bytes: bytes, key4: str) -> bytes:
    """
    Extract only the target color pixels, render as black-on-white,
    scale 2x for better recognition.
    """
    img = Image.open(io.BytesIO(img_bytes)).convert("RGB")
    arr = np.array(img)
    r = arr[:, :, 0].astype(float)
    g = arr[:, :, 1].astype(float)
    b = arr[:, :, 2].astype(float)

    if key4 == "01":
        # Red characters: R dominant over G and B
        mask = (r > 150) & (r > g * 2) & (r > b * 2)
    elif key4 == "03":
        # Blue characters: B dominant over R and G
        mask = (b > 150) & (b > r * 2) & (b > g * 2)
    else:
        # No color filter needed
        return img_bytes

    # Black text on white background
    out = np.ones_like(arr) * 255
    out[mask] = [0, 0, 0]

    result = Image.fromarray(out.astype(np.uint8))

    # Scale 2x for better OCR accuracy
    w, h = result.size
    result = result.resize((w * 2, h * 2), Image.LANCZOS)

    buf = io.BytesIO()
    result.save(buf, format="PNG")
    return buf.getvalue()


def solve_captcha(img_base64: str, key4: str) -> str:
    """
    Solve a captcha image.

    Args:
        img_base64: base64-encoded PNG image (from yzmQuery key1)
        key4: captcha rule code ('00', '01', '02', '03')

    Returns:
        Recognized text, uppercased.
    """
    img_bytes = base64.b64decode(img_base64)

    # Color filter for key4=01/03
    if key4 in ("01", "03"):
        img_bytes = _color_filter(img_bytes, key4)

    ocr = _get_ocr()
    result = ocr.classification(img_bytes)
    return result.upper()


def solve_captcha_from_file(image_path: str, key4: str) -> str:
    """
    Convenience: solve from a file path instead of base64.
    """
    img_bytes = open(image_path, "rb").read()
    img_b64 = base64.b64encode(img_bytes).decode("ascii")
    return solve_captcha(img_b64, key4)


if __name__ == "__main__":
    import sys
    import json

    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <image_path_or_base64> <key4>")
        print(f"  key4: 00=all_chars, 01=red_only, 02=all_chars, 03=blue_only")
        sys.exit(1)

    source = sys.argv[1]
    key4 = sys.argv[2]

    if source.endswith((".png", ".jpg", ".jpeg", ".bmp")):
        result = solve_captcha_from_file(source, key4)
    else:
        result = solve_captcha(source, key4)

    print(json.dumps({"key4": key4, "result": result}))
