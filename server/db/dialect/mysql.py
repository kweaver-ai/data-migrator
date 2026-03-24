#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""MySQL 方言"""
from logging import Logger

from server.config.models import RDSConfig
from server.db.dialect.mariadb import MariaDBDialect


class MysqlDialect(MariaDBDialect):
    def __init__(self, rds_config: RDSConfig, logger: Logger):
        super().__init__(rds_config, logger)

    def init_db_config(self):
        pass
