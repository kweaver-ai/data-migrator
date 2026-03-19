#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 方言 - DB 连接 + SQL 模板 + 幂等执行"""
from logging import Logger

from server.db.dialect.base import RDSDialect
from server.db.dialect._parser.mariadb import MariaDBParser
from server.utils.token import next_token, next_tokens


class MariaDBDialect(MariaDBParser, RDSDialect):
    def __init__(self, conn_config: dict, logger: Logger, system_id: str = ""):
        RDSDialect.__init__(self, conn_config, logger, system_id)

        self.SET_DATABASE_SQL = "USE {db_name}"
        self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
        self.CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS {db_name} CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        self.DROP_DATABASE_SQL = "DROP DATABASE IF EXISTS {db_name}"

        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}'"
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_VIEW_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{view_name}'"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = "SHOW INDEX FROM {db_name}.{table_name} WHERE Key_name = '{index_name}'"
        self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""
        self.COLUMN_NAME_FIELD = "COLUMN_NAME"

        self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
        self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} MODIFY COLUMN IF EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
        self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} RENAME COLUMN IF EXISTS {column_name} TO {new_name}"
        self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} DROP COLUMN IF EXISTS {column_name}"

        self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.{table_name} ({index_property}) COMMENT '{index_comment}'"
        self.RENAME_INDEX_SQL = "ALTER TABLE {db_name}.{table_name} RENAME INDEX {index_name} TO {new_name}"
        self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {index_name} ON {db_name}.{table_name}"

        self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_property}"
        self.RENAME_CONSTRAINT_SQL = None
        self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name}"

        self.RENAME_TABLE_SQL = "RENAME TABLE IF EXISTS {db_name}.{table_name} TO {new_name}"
        self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.{table_name}"

    # ── run_sql overrides ────────────────────────────────────────────────────

    def _run_sql_drop_index(self, cursor, current_db, sql, remaining):
        token, remaining = next_token(remaining)
        if token.upper() == "IF":
            _, remaining = next_token(remaining)
            token, remaining = next_token(remaining)
        idx_name = self.get_real_name(token)
        _, remaining = next_token(remaining)  # skip ON
        tbl_token, _ = next_token(remaining)
        tbl_name = self.get_real_name(tbl_token)
        check_sql = self.QUERY_INDEX_SQL.format(db_name=current_db, table_name=tbl_name, index_name=idx_name)
        if self._check_exists(cursor, check_sql):
            cursor.execute(sql)
        elif self.logger:
            self.logger.info(f"[run_sql] index {idx_name} 不存在, 跳过")

    def _run_sql_alter(self, cursor, current_db, sql, remaining):
        token2, remaining2 = next_token(remaining)
        if token2.upper() != "TABLE":
            cursor.execute(sql)
            return

        tbl_token, remaining3 = next_token(remaining2)
        tbl_name = self._parse_object_name(tbl_token)
        action, remaining4 = next_token(remaining3)
        action = action.upper()
        obj_type, remaining5 = next_token(remaining4)
        obj_type_upper = obj_type.upper()

        if action == "ADD" and obj_type_upper == "COLUMN":
            token, _ = next_token(remaining5)
            if token.upper() == "IF":
                _, remaining5 = next_tokens(remaining5, 3)
                token, _ = next_token(remaining5)
            col_name = self.get_real_name(token)
            check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
            if self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] column {col_name} 已存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "DROP" and obj_type_upper == "COLUMN":
            token, remaining6 = next_token(remaining5)
            if token.upper() == "IF":
                _, remaining6 = next_token(remaining6)
                token, _ = next_token(remaining6)
            col_name = self.get_real_name(token)
            check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "MODIFY" and obj_type_upper == "COLUMN":
            token, remaining6 = next_token(remaining5)
            if token.upper() == "IF":
                _, remaining6 = next_token(remaining6)
                token, _ = next_token(remaining6)
            col_name = self.get_real_name(token)
            check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "RENAME" and obj_type_upper == "COLUMN":
            token, remaining6 = next_token(remaining5)
            if token.upper() == "IF":
                _, remaining6 = next_token(remaining6)
                token, _ = next_token(remaining6)
            col_name = self.get_real_name(token)
            check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "RENAME" and obj_type_upper == "INDEX":
            idx_token, _ = next_token(remaining5)
            idx_name = self.get_real_name(idx_token)
            check_sql = self.QUERY_INDEX_SQL.format(db_name=current_db, table_name=tbl_name, index_name=idx_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] index {idx_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "ADD" and obj_type_upper == "CONSTRAINT":
            constraint_token, _ = next_token(remaining5)
            constraint_name = self.get_real_name(constraint_token)
            check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
            if self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] constraint {constraint_name} 已存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "DROP" and obj_type_upper == "CONSTRAINT":
            token, remaining6 = next_token(remaining5)
            if token.upper() == "IF":
                _, remaining6 = next_token(remaining6)
                token, _ = next_token(remaining6)
            constraint_name = self.get_real_name(token)
            check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] constraint {constraint_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        else:
            cursor.execute(sql)

    def _run_sql_rename(self, cursor, current_db, sql, remaining):
        token2, remaining2 = next_token(remaining)
        if token2.upper() != "TABLE":
            cursor.execute(sql)
            return
        token3, remaining3 = next_token(remaining2)
        if token3.upper() == "IF":
            _, remaining3 = next_token(remaining3)
            token3, _ = next_token(remaining3)
        tbl_name = self._parse_object_name(token3)
        check_sql = self.QUERY_TABLE_SQL.format(db_name=current_db, table_name=tbl_name)
        if not self._check_exists(cursor, check_sql):
            if self.logger:
                self.logger.info(f"[run_sql] table {tbl_name} 不存在, 跳过")
        else:
            cursor.execute(sql)
