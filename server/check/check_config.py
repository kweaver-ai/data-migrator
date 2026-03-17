#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验配置模型 - 复用 tools/utils/check_config.py，适配 AppConfig"""
from server.config.models import AppConfig, CheckRulesConfig


class CheckConfig:
    CheckLatest = 1
    CheckRecently = 2
    CheckAll = 3

    def __init__(self, app_config: AppConfig):
        self.DBTypes = app_config.db_types
        self.DATABASES = app_config.databases

        rules = app_config.check_rules
        self.CheckType = rules.check_type
        self.AllowNonePrimaryKey = rules.allow_none_primary_key
        self.AllowForeignKey = rules.allow_foreign_key
        self.AllowPythonException = rules.allow_python_exception
        self.AllowTableCompareDismatch = rules.allow_table_compare_dismatch
