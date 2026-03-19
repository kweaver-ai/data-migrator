#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DM8 (达梦) 方言"""
from logging import Logger

from server.config.models import RDSConfig
from server.db.dialect.base import RDSDialect


class DM8Dialect(RDSDialect):
    def __init__(self, rds_config: RDSConfig, logger: Logger):
        super().__init__(rds_config, logger)

        self.SET_DATABASE_SQL = "SET SCHEMA {db_name}"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = "SELECT * FROM ALL_INDEXES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND index_name='{index_name}'"
        self.QUERY_CONSTRAINT_SQL = "SELECT * FROM ALL_CONSTRAINTS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"

        self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" ADD COLUMN IF NOT EXISTS {column_name} {column_property}"
        self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" MODIFY {column_name} {column_property}"
        self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME COLUMN {column_name} TO {new_name}"
        self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" DROP COLUMN IF EXISTS {column_name}"

        self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.\"{table_name}\" ({index_property})"
        self.RENAME_INDEX_SQL = "ALTER INDEX {db_name}.{index_name} RENAME TO {new_name}"
        self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {db_name}.{index_name}"

        self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" ADD CONSTRAINT {constraint_name} {constraint_property}"
        self.RENAME_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME CONSTRAINT {constraint_name} TO {new_name}"
        self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" DROP CONSTRAINT {constraint_name} CASCADE"

        self.RENAME_TABLE_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME TO {new_name}"
        self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.\"{table_name}\" CASCADE"

    def init_db_config(self):
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute("SP_SET_PARA_VALUE(1,'GROUP_OPT_FLAG',1);")
            cursor.execute("SP_SET_PARA_VALUE(1,'ENABLE_BLOB_CMP_FLAG',1);")
            cursor.execute("SP_SET_PARA_VALUE(1,'PK_WITH_CLUSTER',0);")
            cursor.execute("alter system set 'COMPATIBLE_MODE'=4 spfile;")
            cursor.execute("alter system set 'MVCC_RETRY_TIMES'=15 spfile;")
            cursor.close()
        except Exception as e:
            raise Exception(f"init dm8 config failed, error: {e}")
