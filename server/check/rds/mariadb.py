#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 校验实现 — 继承 MariaDBDialect (DB) + LintMariaDB (静态校验)"""
from logging import Logger

from server.check.rds.base import load_rds_config
from server.check.check_config import CheckConfig
from server.db.dialect.mariadb import MariaDBDialect
from server.lint.rds.mariadb import LintMariaDB


class CheckMariaDB(MariaDBDialect, LintMariaDB):
    """
    MariaDB 完整校验类：
    - MariaDBDialect 提供 DB 连接、run_sql、add_column 等操作
    - LintMariaDB 提供 check_init、check_update、check_column 等静态校验
    """

    def __init__(self, check_config: CheckConfig, logger: Logger, is_primary: bool = True):
        rds_cfg = load_rds_config()["mariadb"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        conn_config = {**section, "DB_TYPE": "MARIADB"}
        MariaDBDialect.__init__(self, conn_config, logger)
        self.check_config = check_config
