#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Lint 抽象基类 — 纯静态 SQL 校验，无 DB 依赖"""
from abc import ABC, abstractmethod
from logging import Logger

from server.verify.check_config import CheckConfig


class LintRDS(ABC):
    """
    纯静态校验基类。
    子类只需实现 check_init / check_update / check_column。
    无 DB 连接，无 rdsdriver 引用，可在无数据库的 CI 环境中运行。
    """

    def __init__(self, check_config: CheckConfig, logger: Logger):
        self.check_config = check_config
        self.logger = logger

    @abstractmethod
    def check_init(self, sql_list: list):
        """校验 init.sql 语句列表的语法合法性"""

    @abstractmethod
    def check_update(self, sql_list: list):
        """校验升级脚本语句列表的语法合法性"""

    @abstractmethod
    def check_column(self, table_name: str, column):
        """校验列定义是否符合当前数据库类型的规范"""
