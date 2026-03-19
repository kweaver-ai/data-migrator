#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DM8 校验实现 — 继承 DM8Dialect (DB) + LintDM8 (静态校验)"""
from logging import Logger

from server.check.rds.base import load_rds_config
from server.check.check_config import CheckConfig
from server.db.dialect.dm8 import DM8Dialect
from server.lint.rds.dm8 import LintDM8


class CheckDM8(DM8Dialect, LintDM8):
    """
    DM8 完整校验类：
    - DM8Dialect 提供 DB 连接、run_sql、add_column 等操作
    - LintDM8 提供 check_init、check_update、check_column 等静态校验
    """

    def __init__(self, check_config: CheckConfig, logger: Logger, is_primary: bool = True):
        rds_cfg = load_rds_config()["dm8"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        conn_config = {**section, "DB_TYPE": "DM8"}
        DM8Dialect.__init__(self, conn_config, logger)
        self.check_config = check_config
