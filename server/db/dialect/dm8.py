#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DM8 (达梦) 方言 - 合并 check 和 migrate 实现"""
from logging import Logger

from server.db.dialect.base import RDSDialect
from server.utils.table_define import Database, Column
from server.utils.token import next_token, next_tokens


class DM8Dialect(RDSDialect):
    def __init__(self, conn_config: dict, logger: Logger, system_id: str = ""):
        super().__init__(conn_config, logger, system_id)

        self.SET_DATABASE_SQL = "SET SCHEMA {db_name}"
        self.QUERY_DATABASES_SQL = "select OWNER from dba_objects where object_type='SCH'"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"

        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}'"
        self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_VIEW_SQL = "SELECT VIEW_NAME FROM ALL_VIEWS WHERE OWNER='{db_name}' AND VIEW_NAME='{view_name}'"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.QUERY_INDEX_SQL = "SELECT * FROM ALL_INDEXES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND index_name='{index_name}'"
        self.QUERY_CONSTRAINT_SQL = "SELECT * FROM ALL_CONSTRAINTS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"
        self.COLUMN_NAME_FIELD = "COLUMN_NAME"

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
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SP_SET_PARA_VALUE(1,'GROUP_OPT_FLAG',1);")
                    cursor.execute("SP_SET_PARA_VALUE(1,'ENABLE_BLOB_CMP_FLAG',1);")
                    cursor.execute("SP_SET_PARA_VALUE(1,'PK_WITH_CLUSTER',0);")
                    cursor.execute("alter system set 'COMPATIBLE_MODE'=4 spfile;")
                    cursor.execute("alter system set 'MVCC_RETRY_TIMES'=15 spfile;")
        except Exception as e:
            raise Exception(f"init dm8 config failed, error: {e}")

    # ── run_sql overrides ────────────────────────────────────────────────────

    def _run_sql_alter(self, cursor, current_db, sql, remaining):
        token2, remaining2 = next_token(remaining)
        token2_upper = token2.upper()

        if token2_upper == "INDEX":
            # ALTER INDEX db.idx RENAME TO new — 无 table_name 无法查存在性
            cursor.execute(sql)
            return

        if token2_upper != "TABLE":
            cursor.execute(sql)
            return

        tbl_token, remaining3 = next_token(remaining2)
        tbl_name = self._parse_object_name(tbl_token)
        action, remaining4 = next_token(remaining3)
        action = action.upper()

        if action == "ADD":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()
            if obj_type_upper == "COLUMN":
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
            elif obj_type_upper == "CONSTRAINT":
                constraint_token, _ = next_token(remaining5)
                constraint_name = self.get_real_name(constraint_token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 已存在, 跳过")
                else:
                    cursor.execute(sql)
            else:
                cursor.execute(sql)

        elif action == "DROP":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()
            if obj_type_upper == "COLUMN":
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
            elif obj_type_upper == "CONSTRAINT":
                constraint_token, _ = next_token(remaining5)
                constraint_name = self.get_real_name(constraint_token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 不存在, 跳过")
                else:
                    cursor.execute(sql)
            else:
                cursor.execute(sql)

        elif action == "MODIFY":
            # DM8: MODIFY col_name ... (无 COLUMN 关键字)
            col_token, _ = next_token(remaining4)
            col_name = self.get_real_name(col_token)
            check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过")
            else:
                cursor.execute(sql)

        elif action == "RENAME":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()
            if obj_type_upper == "COLUMN":
                col_token, _ = next_token(remaining5)
                col_name = self.get_real_name(col_token)
                check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过")
                else:
                    cursor.execute(sql)
            elif obj_type_upper == "CONSTRAINT":
                constraint_token, _ = next_token(remaining5)
                constraint_name = self.get_real_name(constraint_token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 不存在, 跳过")
                else:
                    cursor.execute(sql)
            elif obj_type_upper == "TO":
                # ALTER TABLE db."tbl" RENAME TO new — 表重命名
                check_sql = self.QUERY_TABLE_SQL.format(db_name=current_db, table_name=tbl_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] table {tbl_name} 不存在, 跳过")
                else:
                    cursor.execute(sql)
            else:
                cursor.execute(sql)
        else:
            cursor.execute(sql)

    # ── 名称解析 ─────────────────────────────────────────────────────────────

    def get_real_name(self, name: str) -> str:
        real_name = name.strip(' "\n;')
        if {".", "`", "'"} & set(real_name):
            raise Exception(f"名称中包含不合法字符: {name}")
        return real_name

    def get_real_column_name(self, name: str) -> str:
        real_name = name
        idx = real_name.find("(")
        if idx != -1:
            real_name = real_name[:idx]
        real_name = real_name.strip(' "\n')
        if {".", "`", "'"} & set(real_name):
            raise Exception(f"名称中包含不合法字符: {name}")
        return real_name

    def parse_sql_use_db(self, sql: str):
        tokens, _ = next_tokens(sql, 3)
        if len(tokens) != 3 or tokens[0].upper() != "SET" or tokens[1].upper() != "SCHEMA":
            raise Exception(f"不合法的 SET SCHEMA 语句: {sql}")
        db_name = self.get_real_name(tokens[2])
        return Database(db_name)

    # ── check 专用：列定义解析 ───────────────────────────────────────────────

    def parse_sql_column_define(self, column_name: str, column_sql: str):
        remaining_sql = column_sql
        column_name = self.get_real_column_name(column_name)
        column_type, remaining_sql = next_token(remaining_sql)
        column = Column(column_name, column_type)
        column.ColumnLen, remaining_sql = self._parse_column_len(remaining_sql)

        while remaining_sql != "":
            key, remaining_sql = next_token(remaining_sql)
            key = key.upper()
            if key == "IDENTITY":
                if not remaining_sql.startswith("("):
                    raise Exception(f"IDENTITY 语法错误: {column_sql}")
                idx = remaining_sql.find(")")
                if idx == -1:
                    raise Exception(f"不合法的建表语句, 缺少 ')': {column_sql}")
                identify_define = remaining_sql[1:idx]
                remaining_sql = remaining_sql[idx + 1:].strip()
                identify_tokens = [t.strip() for t in identify_define.split(",") if t.strip()]
                if len(identify_tokens) != 2 or not identify_tokens[0].isdigit() or not identify_tokens[1].isdigit():
                    raise Exception(f"IDENTITY 语法错误: {column_sql}")
                column.ColumnIdentity = f"{key}({identify_tokens[0]}, {identify_tokens[1]})"
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
        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "BYTE", "MEDIUMINT", "BIGINT"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE", "REAL"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "CLOB", "TEXT", "LONG", "LONGVARCHAR"):
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
