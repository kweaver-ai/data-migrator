#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""校验主入口 - 编排 RepoChecker 和 SchemaChecker"""
from logging import Logger

from server.config.models import AppConfig
from server.check.repo_checker import RepoChecker
from server.check.schema_checker import SchemaChecker


class CheckExecutor:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger
        self.repo_checker = RepoChecker(app_config, logger)
        self.schema_checker = SchemaChecker(app_config, logger)

    def run(self):
        """校验主入口：目录结构校验 + SQL 执行校验"""
        self.repo_checker.run()
        self.schema_checker.run()
