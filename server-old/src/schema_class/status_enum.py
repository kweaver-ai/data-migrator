#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/10/18 14:42
@Author  : mario.jiang
@File    : StatusEnum.py
"""
from enum import Enum


class StateEnum(int, Enum):
    # 相同0，新增1，缺失2，差异3
    STATE_SAME = 0
    STATE_ADDED = 1
    STATE_MISSING = 2
    STATE_CHANGED = 3
