#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/17 17:18
@Author  : mario.jiang
@File    : column_class.py
"""
from dataModelManagement.src.schema_class.status_enum import StateEnum


class ColumnClass:

    def __init__(self, column_name: str, status: StateEnum = StateEnum.STATE_SAME):
        self.column_name = column_name
        # 相同0，新增1，缺失2，差异3
        self.status = status
        # 理论上只有status为3时，才有下面两个属性
        self.column_current = ""
        self.column_target = ""

    def to_dict(self):
        return {
            'column_name': self.column_name,
            'status': self.status,
            'column_current': self.column_current,
            'column_target': self.column_target
        }

    def get_result(self) -> str:
        if self.status == StateEnum.STATE_SAME:
            return ""
        elif self.status == StateEnum.STATE_ADDED:
            return f"当前运行环境新增字段{self.column_current}" + "\n"
        elif self.status == StateEnum.STATE_MISSING:
            return f"当前运行环境缺失字段{self.column_target}" + "\n"
        else:
            return "字段存在差异:" + f"当前字段：{self.column_current}，" + f"期望字段：{self.column_target}" + "\n"
