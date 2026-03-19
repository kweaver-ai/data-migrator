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
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = None
        self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_CATALOG='proton' AND CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""

        self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
        self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} MODIFY COLUMN {column_name} {column_property} COMMENT '{column_comment}'"
        self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} RENAME COLUMN {column_name} TO {new_name}"
        self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} DROP COLUMN IF EXISTS {column_name}"

        self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.{table_name} ({index_property}) COMMENT '{index_comment}'"
        self.RENAME_INDEX_SQL = None
        self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {db_name}.{index_name} CASCADE"

        self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_property}"
        self.RENAME_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} RENAME CONSTRAINT {constraint_name} TO {new_name}"
        self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE"

        self.RENAME_TABLE_SQL = "ALTER TABLE IF EXISTS {db_name}.{table_name} RENAME TO {new_name}"
        self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.{table_name} CASCADE"

    def init_db_config(self):
        try:
            conn = self._get_conn()
            with conn.cursor() as cursor:
                cursor.execute("ALTER SYSTEM SET sql_mode='ANSI_QUOTES';")
                cursor.execute("SELECT sys_reload_conf();")
        except Exception as e:
            raise Exception(f"init kdb9 config failed, error: {e}")
