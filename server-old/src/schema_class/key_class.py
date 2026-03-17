#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/18 15:35
@Author  : mario.jiang
@File    : key_class.py
"""
from dataModelManagement.src.schema_class.status_enum import StateEnum


class KeyClass:
    def __init__(self, key_name: str, status: StateEnum = StateEnum.STATE_SAME):
        self.key_name = key_name
        # 相同0，新增1，缺失2，差异3
        self.status = status
        # 理论上只有status为3时，才有下面两个属性
        self.key_current = ""
        self.key_target = ""

    def to_dict(self):
        return {
            'column_name': self.key_name,
            'status': self.status,
            'column_current': self.key_current,
            'column_target': self.key_target
        }

    def get_result(self) -> str:
        if self.status == StateEnum.STATE_SAME:
            return ""
        elif self.status == StateEnum.STATE_ADDED:
            return f"当前运行环境新增索引{self.key_current}" + "\n"
        elif self.status == StateEnum.STATE_MISSING:
            return f"当前运行环境缺失索引{self.key_target}" + "\n"
        else:
            return "索引存在差异:" + f"当前索引：{self.key_current}，" + f"期望索引：{self.key_target}" + "\n"
