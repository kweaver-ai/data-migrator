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
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = "SELECT * FROM ALL_INDEXES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND index_name='{index_name}'"
        self.QUERY_CONSTRAINT_SQL = "SELECT * FROM ALL_CONSTRAINTS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"

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
