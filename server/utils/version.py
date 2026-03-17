#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""版本号工具 - 合并 tools/utils/version.py + server-old/src/utils/util.py"""
import re
from typing import List, Optional


def is_version_dir(version_str: str) -> bool:
    """判断目录名是否为合法版本号"""
    try:
        [int(n) for n in version_str.split(".")]
        return True
    except (ValueError, AttributeError):
        return False


def compare_version(v1: str, v2: str) -> int:
    """
    比较两个版本号，返回 -1/0/1。
    支持格式: 1.0.0, 1.4.20.1, m5.5, m5-6
    """
    # m5.5 格式
    match1 = re.match(r"m(\d+)\.(\d+)", v1)
    match2 = re.match(r"m(\d+)\.(\d+)", v2)
    if match1 and match2:
        major1, minor1 = int(match1.group(1)), float(match1.group(2))
        major2, minor2 = int(match2.group(1)), float(match2.group(2))
        if major1 != major2:
            return -1 if major1 < major2 else 1
        if minor1 != minor2:
            return -1 if minor1 < minor2 else 1
        return 0

    # m5-6 格式
    match1 = re.match(r"m(\d+)-(\d+)", v1)
    match2 = re.match(r"m(\d+)-(\d+)", v2)
    if match1 and match2:
        major1, minor1 = int(match1.group(1)), int(match1.group(2))
        major2, minor2 = int(match2.group(1)), int(match2.group(2))
        if major1 != major2:
            return -1 if major1 < major2 else 1
        if minor1 != minor2:
            return -1 if minor1 < minor2 else 1
        return 0

    # 标准语义化版本: 1.0.0, 1.4.20.1
    try:
        arr1 = [int(n) for n in v1.split(".")]
        arr2 = [int(n) for n in v2.split(".")]
        max_len = max(len(arr1), len(arr2))
        arr1 += [0] * (max_len - len(arr1))
        arr2 += [0] * (max_len - len(arr2))
        for a, b in zip(arr1, arr2):
            if a > b:
                return 1
            elif a < b:
                return -1
        return 0
    except Exception as ex:
        raise Exception(f"Unable to parse version number, details: {ex}")


def get_max_version(versions: List[str]) -> Optional[str]:
    """返回列表中的最大版本号"""
    if not versions:
        return None
    max_v = versions[0]
    for v in versions[1:]:
        if compare_version(v, max_v) > 0:
            max_v = v
    return max_v


def get_min_version(versions: List[str]) -> Optional[str]:
    """返回列表中的最小版本号"""
    if not versions:
        return None
    min_v = versions[0]
    for v in versions[1:]:
        if compare_version(v, min_v) < 0:
            min_v = v
    return min_v


def sort_versions(versions: List[str]) -> List[str]:
    """对版本号列表排序（升序）"""
    arr = list(versions)
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1
        while j >= 0 and compare_version(arr[j], key) > 0:
            arr[j + 1] = arr[j]
            j -= 1
        arr[j + 1] = key
    return arr


def extract_number(file_path: str) -> int:
    """从文件名中提取序号，如 01-xxx.sql -> 1"""
    s = file_path.split("/")[-1]
    match = re.search(r"^(\d+)-.*\.(sql|py|json)$", s)
    if match:
        return int(match.group(1))
    raise Exception(f"The upgrade file name must match NN-xxx.sql|py|json, filename: {s}")


class VersionUtil:
    """版本号对象，支持比较和排序"""

    def __init__(self, version_str: str):
        self.VersionStr = version_str
        self.Version = [int(n) for n in version_str.split(".")]

    def __str__(self):
        return self.VersionStr

    def __repr__(self):
        return self.VersionStr

    def __lt__(self, other):
        max_len = max(len(self.Version), len(other.Version))
        arr1 = self.Version + [0] * (max_len - len(self.Version))
        arr2 = other.Version + [0] * (max_len - len(other.Version))
        for a, b in zip(arr1, arr2):
            if a != b:
                return a < b
        return False

    def __ge__(self, other):
        max_len = max(len(self.Version), len(other.Version))
        arr1 = self.Version + [0] * (max_len - len(self.Version))
        arr2 = other.Version + [0] * (max_len - len(other.Version))
        for a, b in zip(arr1, arr2):
            if a != b:
                return a > b
        return True

    def __eq__(self, other):
        max_len = max(len(self.Version), len(other.Version))
        arr1 = self.Version + [0] * (max_len - len(self.Version))
        arr2 = other.Version + [0] * (max_len - len(other.Version))
        return arr1 == arr2

    def __hash__(self):
        return hash(self.VersionStr)
