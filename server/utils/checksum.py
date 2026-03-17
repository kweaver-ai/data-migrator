#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SHA-256 文件哈希"""
import hashlib


def sha256_file(file_path: str) -> str:
    """计算文件的 SHA-256 校验和"""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
