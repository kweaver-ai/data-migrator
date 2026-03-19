#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 方言 - 合并 check 和 migrate 实现"""
from logging import Logger

from server.db.dialect.base import RDSDialect
from server.utils.table_define import Database, Column
from server.utils.token import next_token, next_tokens


class MariaDBDialect(RDSDialect):
    def __init__(self, conn_config: dict, logger: Logger, system_id: str = ""):
        super().__init__(conn_config, logger, system_id)

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
        # MariaDB: DROP INDEX [IF EXISTS] <idx> ON <tbl>
        token, remaining = next_token(remaining)
        if token.upper() == "IF":
            _, remaining = next_token(remaining)  # skip EXISTS
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
        # RENAME TABLE [IF EXISTS] db.tbl TO new_name
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

    # ── 名称解析 ─────────────────────────────────────────────────────────────

    def get_real_name(self, name: str) -> str:
        real_name = name.strip(" `\n;")
        if {".", '"', "'"} & set(real_name):
            raise Exception(f"名称中包含不合法字符: {name}")
        return real_name

    def get_real_column_name(self, name: str) -> str:
        real_name = name
        idx = real_name.find("(")
        if idx != -1:
            real_name = real_name[:idx]
        real_name = real_name.strip(" `\n")
        if {".", '"', "'"} & set(real_name):
            raise Exception(f"名称中包含不合法字符: {name}")
        return real_name

    def parse_sql_use_db(self, sql: str):
        tokens, _ = next_tokens(sql, 2)
        if len(tokens) != 2 or tokens[0].upper() != "USE":
            raise Exception(f"不合法的 USE 语句: {sql}")
        db_name = self.get_real_name(tokens[1])
        return Database(db_name)

    # ── check 专用：列定义解析 ───────────────────────────────────────────────

    def parse_sql_column_define(self, column_name: str, column_sql: str):
        remaining_sql = column_sql
        column_name = self.get_real_column_name(column_name)
        column_type, remaining_sql = next_token(remaining_sql)
        column = Column(column_name, column_type)
        column.ColumnLen, remaining_sql = self._parse_column_len(remaining_sql)
        column.ColumnUnsigned, remaining_sql = self._parse_column_unsigned(remaining_sql)

        while remaining_sql != "":
            key, remaining_sql = next_token(remaining_sql)
            key = key.upper()
            if key == "AUTO_INCREMENT":
                column.ColumnIdentity = key
            elif key == "CHARACTER":
                key2, remaining_sql = next_token(remaining_sql)
                if key2.upper() != "SET":
                    raise Exception(f"CHARACTER SET 语法错误: {column_sql}")
                column.ColumnCharset, remaining_sql = next_token(remaining_sql)
            elif key == "COLLATE":
                column.ColumnCollate, remaining_sql = next_token(remaining_sql)
            elif key == "COMMENT":
                column.ColumnComment, remaining_sql = next_token(remaining_sql)
            elif key == "NULL":
                column.ColumnNull = True
            elif key == "NOT":
                key2, remaining_sql = next_token(remaining_sql)
                if key2.upper() != "NULL":
                    raise Exception(f"NOT NULL 语法错误: {column_sql}")
                column.ColumnNull = False
            elif key == "DEFAULT":
                column.ColumnDefault, remaining_sql = self._parse_default_value(remaining_sql, column_sql)
            else:
                raise Exception(f"列定义中包含不合法的关键字 '{key}': {column_sql}")
        return column

    def get_column_type(self, column: dict) -> tuple:
        data_type = column["DATA_TYPE"].upper()
        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT", "BOOLEAN"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "TINYBLOB", "BLOB",
                           "MEDIUMBLOB", "LONGBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"):
            return data_type, "StringType"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            return data_type, "DateAndTimeType"
        return data_type, "UNKNOWN"

    # ── 内部辅助 ─────────────────────────────────────────────────────────────

    def _parse_column_len(self, column_sql: str):
        if column_sql.startswith("("):
            idx = column_sql.find(")")
            if idx != -1:
                return column_sql[1:idx].strip(), column_sql[idx + 1:].strip()
        return None, column_sql

    def _parse_column_unsigned(self, column_sql: str):
        token, remaining = next_token(column_sql)
        if token.upper().startswith("UNSIGNED"):
            return True, remaining.strip()
        return False, column_sql

    def _parse_default_value(self, remaining_sql: str, column_sql: str):
        default_value, remaining_sql = next_token(remaining_sql)
        if remaining_sql.startswith("("):
            stack, end_idx = [], 0
            for i, char in enumerate(remaining_sql):
                if char == "(":
                    stack.append(char)
                elif char == ")":
                    stack.pop()
                    if not stack:
                        end_idx = i
                        break
            else:
                raise Exception(f"不合法的建表语句, 缺少 ')': {column_sql}")
            default_value += remaining_sql[:end_idx + 1]
            remaining_sql = remaining_sql[end_idx + 1:].strip()
        return default_value, remaining_sql
