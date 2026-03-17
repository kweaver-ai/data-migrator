#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/8/9 15:47
@Author  : mario.jiang
@File    : micro_service.py
"""
from typing import List

from dataModelManagement.src.schema_class.table_class import TableClass


class MicroServiceClass:
    def __init__(self, service_name="", add_tables: List[TableClass] = None, miss_tables: List[TableClass] = None,
                 diff_tables: List[TableClass] = None, status: int = 0, service_version=""
                 ):
        self.service_name = service_name
        self.add_tables = add_tables
        self.miss_tables = miss_tables
        self.diff_tables = diff_tables
        # 0正常返回结果(默认) 1新增 2缺失
        self.status = status
        # 微服务的版本
        self.service_version = service_version

    def to_dict(self):
        add_tables_dicts = [add_table.to_dict() for add_table in self.add_tables]
        miss_tables_dicts = [miss_table.to_dict() for miss_table in self.miss_tables]
        diff_tables_dicts = [diff_table.to_dict() for diff_table in self.diff_tables]
        return {
            'service_name': self.service_name,
            'add_tables': add_tables_dicts,
            'miss_tables': miss_tables_dicts,
            'diff_tables': diff_tables_dicts,
            'status': self.status,
            'service_version': self.service_version
        }

    def get_result(self) -> str:
        add_table_list = [add_table.get_result() for add_table in self.add_tables]
        miss_table_list = [miss_table.get_result() for miss_table in self.miss_tables]
        diff_table_list = [diff_table.get_result() for diff_table in self.diff_tables]
        miss_tables = ""
        add_tables = ""
        diff_tables = ""
        if len(miss_table_list) > 0:
            miss_tables = "缺失的表：" + "".join(miss_table_list) + "\n"
        if len(add_table_list) > 0:
            add_tables = "新增的表：" + "".join(add_table_list) + "\n"
        if len(diff_table_list) > 0:
            diff_tables = "有差异的表：" + "".join(diff_table_list) + "\n"
        result_str = miss_tables + add_tables + diff_tables

        return result_str

