#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/17 17:13
@Author  : mario.jiang
@File    : table_class.py
"""
from typing import List

from dataModelManagement.src.schema_class.column_class import ColumnClass
from dataModelManagement.src.schema_class.constraint_class import ConstraintClass
from dataModelManagement.src.schema_class.key_class import KeyClass
from dataModelManagement.src.schema_class.status_enum import StateEnum
from dataModelManagement.src.schema_class.unique_class import UniqueClass


class TableClass:

    def __init__(self, table_name):
        # 表名称
        self.table_name = table_name
        # 主键
        # 主键标识字段 0无差异(默认) 1新增 2缺失 3差异
        self.primary_status = StateEnum.STATE_SAME
        self.target_primary = ""
        self.current_primary = ""
        # 唯一约束
        self.uniques: List[UniqueClass] = []
        # 字段
        self.columns: List[ColumnClass] = []
        # key
        self.keys: List[KeyClass] = []
        # constraint
        self.constraints: List[ConstraintClass] = []
        # # 表说明
        # # 表说明标识字段 0无差异(默认) 1新增 2缺失 3差异(保留字段)
        # 标记位 标记表结构差异 1:constraint, 2:key, 4:unique, 8:primary, 16:column    其他数值为以上排列组合，例如31表示该表拥有所有差异
        self.sign = 0

    def get_sign(self):
        return self.sign

    def to_dict(self):
        unique_dicts = [unique.to_dict() for unique in self.uniques]
        columns_dicts = [column.to_dict() for column in self.columns]
        key_dicts = [key.to_dict() for key in self.keys]
        constraints_dicts = [constraint.to_dict() for constraint in self.constraints]
        return {
            'table_name': self.table_name,
            'primary_status': self.primary_status,
            'target_primary': self.target_primary,
            'current_primary': self.current_primary,
            'uniques': unique_dicts,
            'columns': columns_dicts,
            'keys': key_dicts,
            'constraints': constraints_dicts,
            'sign': self.sign
        }

    def get_primary_result(self) -> str:
        if self.primary_status == StateEnum.STATE_SAME:
            return ""
        elif self.primary_status == StateEnum.STATE_ADDED:
            return f"当前运行环境新增主键{self.current_primary}" + "\n"
        elif self.primary_status == StateEnum.STATE_MISSING:
            return f"当前运行环境缺失主键{self.target_primary}" + "\n"
        else:
            return "主键存在差异:" + f"当前主键：{self.current_primary}，" + f"期望主键：{self.target_primary}" + "\n"

    def get_result(self) -> str:
        unique_results = [unique.get_result() for unique in self.uniques]
        columns_results = [column.get_result() for column in self.columns]
        key_results = [key.get_result() for key in self.keys]
        constraints_results = [constraint.get_result() for constraint in self.constraints]
        results = unique_results + columns_results + key_results + constraints_results

        result_str = f"表名：{self.table_name}" + "\n" + self.get_primary_result() + "".join(results)
        return result_str
