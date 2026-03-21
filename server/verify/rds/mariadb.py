#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""MariaDB 校验实现 — 继承 MariaDBDialect (DB) + LintMariaDB (静态校验)"""
from logging import Logger

from server.verify.check_config import CheckConfig
from server.db.dialect.mariadb import MariaDBDialect
from server.lint.rds.mariadb import LintMariaDB


class CheckMariaDB(MariaDBDialect, LintMariaDB):
    """
    MariaDB 完整校验类：
    - MariaDBDialect 提供 DB 连接、run_sql、add_column 等操作
    - LintMariaDB 提供 check_init、check_update、check_column 等静态校验
    """

    def __init__(self, conn_config: dict, check_config: CheckConfig, logger: Logger):
        MariaDBDialect.__init__(self, conn_config, logger)
        self.check_config = check_config
