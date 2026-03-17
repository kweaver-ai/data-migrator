#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/17 17:13
@Author  : mario.jiang
@File    : database_class.py
"""


class DataBaseClass:
    def __init__(self, database_name="", add_tables: list = None, miss_tables: list = None, diff_tables: list = None,
                 status: int = 0
                 ):
        self.database_name = database_name
        self.add_tables = add_tables
        self.miss_tables = miss_tables
        self.diff_tables = diff_tables
        # 0正常返回结果(默认) 1新增 2缺失
        self.status = status

    def to_dict(self):
        return self.__dict__

