#!/usr/bin/env python3
"""
247 上调用 173 OCR 服务的轻量客户端。

给验证码前处理脚本复用：
- 单图识别
- 批量识别
"""

from __future__ import annotations

import base64
from pathlib import Path
from typing import Iterable, List, Optional

import requests


class OCRServiceClient:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _headers(self):
        h = {"Content-Type": "application/json"}
        if self.token:
            h["X-OCR-Token"] = self.token
        return h

    @staticmethod
    def _b64_from_file(path: str | Path) -> str:
        data = Path(path).read_bytes()
        return base64.b64encode(data).decode("ascii")

    def healthz(self):
        r = requests.get(f"{self.base_url}/healthz", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def ocr_char(self, image_path: str | Path, hint: Optional[str] = None, topk: int = 5, lang: Optional[str] = None):
        payload = {
            "image_b64": self._b64_from_file(image_path),
            "hint": hint,
            "topk": topk,
            "lang": lang,
        }
        r = requests.post(f"{self.base_url}/ocr/char", json=payload, headers=self._headers(), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def ocr_batch(self, items: Iterable[tuple[str, str | Path]], hint: Optional[str] = None, topk: int = 5, lang: Optional[str] = None):
        payload = {
            "items": [
                {"id": item_id, "image_b64": self._b64_from_file(path)}
                for item_id, path in items
            ],
            "hint": hint,
            "topk": topk,
            "lang": lang,
        }
        r = requests.post(f"{self.base_url}/ocr/batch", json=payload, headers=self._headers(), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def ocr_rec_char(self, image_path: str | Path, hint: Optional[str] = None, topk: int = 5, lang: Optional[str] = None):
        payload = {
            "image_b64": self._b64_from_file(image_path),
            "hint": hint,
            "topk": topk,
            "lang": lang,
        }
        r = requests.post(f"{self.base_url}/ocr/rec_char", json=payload, headers=self._headers(), timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def ocr_rec_batch(self, items: Iterable[tuple[str, str | Path]], hint: Optional[str] = None, topk: int = 5, lang: Optional[str] = None):
        payload = {
            "items": [
                {"id": item_id, "image_b64": self._b64_from_file(path)}
                for item_id, path in items
            ],
            "hint": hint,
            "topk": topk,
            "lang": lang,
        }
        r = requests.post(f"{self.base_url}/ocr/rec_batch", json=payload, headers=self._headers(), timeout=self.timeout)
        r.raise_for_status()
        return r.json()


if __name__ == "__main__":
    import json
    import os
    import sys

    base = os.environ.get("OCR_BASE_URL", "http://localhost:17861")
    token = os.environ.get("OCR_API_TOKEN")
    cli = OCRServiceClient(base_url=base, token=token)

    if len(sys.argv) == 1:
        print(json.dumps(cli.healthz(), ensure_ascii=False, indent=2))
    else:
        path = sys.argv[1]
        print(json.dumps(cli.ocr_char(path), ensure_ascii=False, indent=2))
