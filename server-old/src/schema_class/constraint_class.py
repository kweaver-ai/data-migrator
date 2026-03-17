#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/18 15:56
@Author  : mario.jiang
@File    : constraint_class.py
"""
from dataModelManagement.src.schema_class.status_enum import StateEnum


class ConstraintClass:
    def __init__(self, constraint_name: str, status: StateEnum = StateEnum.STATE_SAME):
        self.constraint_name = constraint_name
        # 相同0，新增1，缺失2，差异3
        self.status = status
        # 理论上只有status为3时，才有下面两个属性
        self.constraint_current = ""
        self.constraint_target = ""

    def to_dict(self):
        return {
            'column_name': self.constraint_name,
            'status': self.status,
            'column_current': self.constraint_current,
            'column_target': self.constraint_target
        }

    def get_result(self) -> str:
        if self.status == StateEnum.STATE_SAME:
            return ""
        elif self.status == StateEnum.STATE_ADDED:
            return f"当前运行环境新增约束{self.constraint_current}" + "\n"
        elif self.status == StateEnum.STATE_MISSING:
            return f"当前运行环境缺失约束{self.constraint_target}" + "\n"
        else:
            return "约束存在差异:" + f"当前约束：{self.constraint_current}，" + f"期望约束：{self.constraint_target}" + "\n"
