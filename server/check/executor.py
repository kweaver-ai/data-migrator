#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验主入口 - 编排 RepoChecker 和 SchemaChecker"""
from logging import Logger

from server.config.models import AppConfig


class CheckExecutor:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger

    def run(self):
        """校验主入口：目录结构校验 + SQL 执行校验"""
        
        from server.check.repo_checker import RepoChecker
        repo_checker = RepoChecker(self.app_config, self.logger)
        repo_checker.run()

        # schema_checker 依赖 rdsdriver 数据库连接，延迟导入
        from server.check.schema_checker import SchemaChecker
        schema_checker = SchemaChecker(self.app_config, self.logger)
        schema_checker.run()
