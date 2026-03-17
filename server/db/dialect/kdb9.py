#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KDB9 (人大金仓) 方言"""
from logging import Logger

from server.config.models import RDSConfig
from server.db.dialect.base import RDSDialect


class KDB9Dialect(RDSDialect):
    def __init__(self, rds_config: RDSConfig, logger: Logger):
        super().__init__(rds_config, logger)

        self.SET_DATABASE_SQL = "SET SEARCH_PATH TO {db_name}"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = None
        self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_CATALOG='proton' AND CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""

    def init_db_config(self):
        try:
            conn = self._get_conn()
            with conn.cursor() as cursor:
                cursor.execute("ALTER SYSTEM SET sql_mode='ANSI_QUOTES';")
                cursor.execute("SELECT sys_reload_conf();")
        except Exception as e:
            raise Exception(f"init kdb9 config failed, error: {e}")
