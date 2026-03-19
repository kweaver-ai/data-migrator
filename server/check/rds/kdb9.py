#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KDB9 校验实现 — 继承 KDB9Dialect (DB) + LintKDB9 (静态校验)"""
from logging import Logger

from server.check.rds.base import load_rds_config
from server.check.check_config import CheckConfig
from server.db.dialect.kdb9 import KDB9Dialect
from server.lint.rds.kdb9 import LintKDB9


class CheckKDB9(KDB9Dialect, LintKDB9):
    """
    KDB9 完整校验类：
    - KDB9Dialect 提供 DB 连接、run_sql、add_column 等操作
    - LintKDB9 提供 check_init、check_update、check_column 等静态校验
    """

    def __init__(self, check_config: CheckConfig, logger: Logger = None, is_primary: bool = True):
        rds_cfg = load_rds_config()["kdb9"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        conn_config = {**section, "DB_TYPE": "KDB9"}
        KDB9Dialect.__init__(self, conn_config, logger)
        self.check_config = check_config
