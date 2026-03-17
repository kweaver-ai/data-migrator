#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def is_version_dir(versionStr: str):
  try:
    version = [int(n) for n in versionStr.split('.')]
    return True
  except Exception as e:
    return False


class VersionUtil:
  VersionStr = ""
  Version = []

  def __init__(self, versionStr: str):
    self.VersionStr = versionStr
    self.Version = [int(n) for n in versionStr.split('.')]

  def __str__(self):
    return self.VersionStr

  def __repr__(self):
    return self.VersionStr

  def __lt__(self, other):
    # 补零对齐长度
    max_len = max(len(self.Version), len(other.Version))
    arr1 = self.Version + [0] * (max_len - len(self.Version))
    arr2 = other.Version + [0] * (max_len - len(other.Version))
    # 逐位比较
    for a, b in zip(arr1, arr2):
      if a != b:
        return a < b
    return False  # 所有位相等时，长度更长的更大（这里补零后长度一致，故返回False）

  def __ge__(self, other):
    # 补零对齐长度
    max_len = max(len(self.Version), len(other.Version))
    arr1 = self.Version + [0] * (max_len - len(self.Version))
    arr2 = other.Version + [0] * (max_len - len(other.Version))
    # 逐位比较
    for a, b in zip(arr1, arr2):
      if a != b:
        return a > b
    return True  # 所有位相等时，返回 True（等于也算满足大于等于）
