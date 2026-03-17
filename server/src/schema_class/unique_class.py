#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/18 14:27
@Author  : mario.jiang
@File    : unique_class.py
"""
from dataModelManagement.src.schema_class.status_enum import StateEnum


class UniqueClass:
    def __init__(self, unique_name: str, status: StateEnum = StateEnum.STATE_SAME):
        self.unique_name = unique_name
        # 相同0，新增1，缺失2，差异3
        self.status = status
        # 理论上只有status为3时，才有下面两个属性
        self.unique_current = ""
        self.unique_target = ""

    def to_dict(self):
        return {
            'column_name': self.unique_name,
            'status': self.status,
            'column_current': self.unique_current,
            'column_target': self.unique_target
        }

    def get_result(self) -> str:
        if self.status == StateEnum.STATE_SAME:
            return ""
        elif self.status == StateEnum.STATE_ADDED:
            return f"当前运行环境新增唯一约束{self.unique_current}" + "\n"
        elif self.status == StateEnum.STATE_MISSING:
            return f"当前运行环境缺失唯一约束{self.unique_target}" + "\n"
        else:
            return "唯一约束存在差异:" + f"当前唯一约束：{self.unique_current}，" + f"期望唯一约束：{self.unique_target}" + "\n"
