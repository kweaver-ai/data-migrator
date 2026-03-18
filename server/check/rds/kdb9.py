#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KDB9 校验实现 — 移植自 tools/rds/kdb9.py"""
from logging import Logger

from server.check.rds.base import CheckRDS, load_rds_config
from server.check.check_config import CheckConfig
from server.utils.table_define import Database, Table, Index, PrimaryIndex, UniqueIndex, Column
from server.utils.token import next_token, next_tokens


class CheckKDB9(CheckRDS):
    def __init__(self, check_config: CheckConfig, logger: Logger = None, is_primary: bool = True):
        self.DB_TYPE = "KDB9"

        rds_cfg = load_rds_config()["kdb9"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        self.DB_CONFIG_ROOT = {**section, "DB_TYPE": self.DB_TYPE}

        self.SET_DATABASE_SQL = "SET SEARCH_PATH TO {db_name}"
        self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name='proton'"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"
        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}'"
        self.QUERY_TABLE_SQL = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        )
        self.QUERY_VIEW_SQL = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{view_name}'"
        )
        self.RENAME_TABLE_SQL = "ALTER TABLE IF EXISTS {db_name}.{table_name} RENAME TO {new_name}"
        self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.{table_name} CASCADE"

        self.COLUMN_NAME_FIELD = "COLUMN_NAME"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
        self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
        self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} MODIFY COLUMN {column_name} {column_property} COMMENT '{column_comment}'"
        self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} RENAME COLUMN {column_name} TO {new_name}"
        self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} DROP COLUMN IF EXISTS {column_name}"

        self.QUERY_INDEX_SQL = None
        self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.{table_name} ({index_property}) COMMENT '{index_comment}'"
        self.RENAME_INDEX_SQL = None
        self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {db_name}.{index_name} CASCADE"

        self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_CATALOG='proton' AND CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""
        self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_property}"
        self.RENAME_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} RENAME CONSTRAINT {constraint_name} TO {new_name}"
        self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name} CASCADE"

        super().__init__(check_config, logger)

    def _run_sql_alter(self, cursor, current_db, sql, remaining):
        token2, remaining2 = next_token(remaining)
        if token2.upper() != "TABLE":
            cursor.execute(sql)
            return

        # KDB9: ALTER TABLE [IF EXISTS] db.tbl ...
        tbl_token, remaining3 = next_token(remaining2)
        if tbl_token.upper() == "IF":
            _, remaining3 = next_token(remaining3)  # skip EXISTS
            tbl_token, remaining3 = next_token(remaining3)
        tbl_name = self._parse_object_name(tbl_token)
        action, remaining4 = next_token(remaining3)
        action = action.upper()

        if action == "ADD":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()

            if obj_type_upper == "COLUMN":
                # ADD COLUMN [IF NOT EXISTS] col_name ...
                token, _ = next_token(remaining5)
                if token.upper() == "IF":
                    _, remaining5 = next_tokens(remaining5, 3)  # skip IF NOT EXISTS
                    token, _ = next_token(remaining5)
                col_name = self.get_real_name(token)
                check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
                if self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] column {col_name} 已存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            elif obj_type_upper == "CONSTRAINT":
                # ADD CONSTRAINT constraint_name ...
                constraint_token, _ = next_token(remaining5)
                constraint_name = self.get_real_name(constraint_token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 已存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            else:
                cursor.execute(sql)

        elif action == "DROP":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()

            if obj_type_upper == "COLUMN":
                # DROP COLUMN [IF EXISTS] col_name
                token, remaining6 = next_token(remaining5)
                if token.upper() == "IF":
                    _, remaining6 = next_token(remaining6)  # skip EXISTS
                    token, _ = next_token(remaining6)
                col_name = self.get_real_name(token)
                check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            elif obj_type_upper == "CONSTRAINT":
                # DROP CONSTRAINT [IF EXISTS] constraint_name [CASCADE]
                token, remaining6 = next_token(remaining5)
                if token.upper() == "IF":
                    _, remaining6 = next_token(remaining6)  # skip EXISTS
                    token, _ = next_token(remaining6)
                constraint_name = self.get_real_name(token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            else:
                cursor.execute(sql)

        elif action == "MODIFY":
            # KDB9: MODIFY COLUMN col_name ...
            obj_type, remaining5 = next_token(remaining4)
            if obj_type.upper() == "COLUMN":
                col_token, _ = next_token(remaining5)
                col_name = self.get_real_name(col_token)
                check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)
            else:
                cursor.execute(sql)

        elif action == "RENAME":
            obj_type, remaining5 = next_token(remaining4)
            obj_type_upper = obj_type.upper()

            if obj_type_upper == "COLUMN":
                # RENAME COLUMN col_name TO new_name
                col_token, _ = next_token(remaining5)
                col_name = self.get_real_name(col_token)
                check_sql = self.QUERY_COLUMN_SQL.format(db_name=current_db, table_name=tbl_name, column_name=col_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] column {col_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            elif obj_type_upper == "CONSTRAINT":
                # RENAME CONSTRAINT constraint_name TO new_name
                constraint_token, _ = next_token(remaining5)
                constraint_name = self.get_real_name(constraint_token)
                check_sql = self.QUERY_CONSTRAINT_SQL.format(db_name=current_db, table_name=tbl_name, constraint_name=constraint_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] constraint {constraint_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            elif obj_type_upper == "TO":
                # ALTER TABLE [IF EXISTS] db.tbl RENAME TO new — 表重命名
                check_sql = self.QUERY_TABLE_SQL.format(db_name=current_db, table_name=tbl_name)
                if not self._check_exists(cursor, check_sql):
                    if self.logger:
                        self.logger.info(f"[run_sql] table {tbl_name} 不存在, 跳过: {sql}")
                else:
                    cursor.execute(sql)

            else:
                cursor.execute(sql)

        else:
            cursor.execute(sql)

    def get_column_type(self, column: dict) -> tuple:
        data_type = column["DATA_TYPE"].upper()
        if data_type == "USER-DEFINED":
            data_type = column.get("COLUMN_TYPE", "").upper()

        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE", "REAL"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("BOOLEAN",):
            return data_type, "BooleanType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "TEXT",
                           "TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT"):
            return data_type, "StringType"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            return data_type, "DateAndTimeType"
        return data_type, "UNKNOWN"

    # ── check_init / check_update ──

    def check_init(self, sql_list: list):
        if len(sql_list) == 0:
            return

        sql = sql_list[0]
        token, remaining_sql = next_token(sql)
        token = token.upper()
        if token != "SET":
            raise Exception(f"init文件中第一条语句必须为 'SET SEARCH_PATH TO': {sql}")

        db = self.parse_sql_use_db(sql)
        if db is None:
            raise Exception(f"USE Database语法错误: {sql}")

        sql_list = sql_list[1:]
        for sql in sql_list:
            token, remaining_sql = next_token(sql)
            token = token.upper()
            if token == "SET":
                db = self.parse_sql_use_db(sql)
            elif token == "CREATE":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 == "TABLE":
                    self.parse_sql_create_table(sql, db)
                elif token2 == "UNIQUE":
                    self.parse_sql_create_unique_index(sql, db)
                elif token2 == "INDEX":
                    self.parse_sql_create_index(sql, db)
                elif token2 == "VIEW":
                    continue
                elif token2 == "OR":
                    tokens, remaining_sql = next_tokens(remaining_sql, 2)
                    tokens = [t.upper() for t in tokens]
                    if tokens != ["REPLACE", "VIEW"]:
                        raise Exception(f"不合法的sql语句, 仅支持 'CREATE OR REPLACE VIEW': {sql}")
                    continue
                else:
                    raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
            elif token == "INSERT":
                continue
            else:
                raise Exception(f"不合法的sql语句, 仅支持 'USE', 'CREATE TABLE', 'INSERT': {sql}")

    def check_update(self, sql_list: list):
        if len(sql_list) == 0:
            return

        sql = sql_list[0]
        token, remaining_sql = next_token(sql)
        token = token.upper()
        if token != "SET":
            raise Exception(f"init文件中第一条语句必须为 'SET SEARCH_PATH TO': {sql}")

        db = self.parse_sql_use_db(sql)
        if db is None:
            raise Exception(f"USE Database语法错误: {sql}")

        sql_list = sql_list[1:]
        for sql in sql_list:
            token, remaining_sql = next_token(sql)
            token = token.upper()
            if token == "SET":
                db = self.parse_sql_use_db(sql)
            elif token == "CREATE":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 == "TABLE":
                    self.parse_sql_create_table(sql, db)
                elif token2 == "UNIQUE":
                    self.parse_sql_create_unique_index(sql, db)
                elif token2 == "INDEX":
                    self.parse_sql_create_index(sql, db)
                elif token2 == "VIEW":
                    continue
                elif token2 == "OR":
                    tokens, remaining_sql = next_tokens(remaining_sql, 2)
                    tokens = [t.upper() for t in tokens]
                    if tokens != ["REPLACE", "VIEW"]:
                        raise Exception(f"不合法的sql语句, 仅支持 'CREATE OR REPLACE VIEW': {sql}")
                    continue
                else:
                    raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
            elif token == "INSERT":
                continue
            elif token == "UPDATE":
                continue
            elif token == "DROP":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 not in ("INDEX", "TABLE", "VIEW"):
                    raise Exception(f"不合法的sql语句, 仅支持 'DROP INDEX', 'DROP TABLE', 'DROP VIEW': {sql}")
                continue
            elif token == "ALTER":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 != "TABLE":
                    raise Exception(f"不合法的sql语句, 仅支持 'ALTER TABLE': {sql}")
                continue
            else:
                raise Exception(f"不合法的sql语句, 仅支持 'USE', 'CREATE TABLE', 'INSERT', 'UPDATE', 'ALTER TABLE', 'DROP INDEX', 'DROP TABLE', 'DROP VIEW': {sql}")

    # ── SQL 解析 ──

    def parse_sql_use_db(self, sql: str):
        remaining_sql = sql
        tokens, remaining_sql = next_tokens(remaining_sql, 4)
        if len(tokens) != 4 or tokens[0].upper() != "SET" or tokens[1].upper() != "SEARCH_PATH" or tokens[2].upper() != "TO":
            raise Exception(f"不合法的 SET SEARCH_PATH TO 语句: {sql}")

        db_name = tokens[3]
        db_name = self.get_real_name(db_name)
        db = Database(db_name)
        return db

    def parse_sql_create_table(self, sql: str, db: Database):
        remaining_sql = sql
        tokens, remaining_sql = next_tokens(remaining_sql, 5)
        if (
            len(tokens) != 5
            or tokens[0].upper() != "CREATE"
            or tokens[1].upper() != "TABLE"
            or tokens[2].upper() != "IF"
            or tokens[3].upper() != "NOT"
            or tokens[4].upper() != "EXISTS"
        ):
            raise Exception(f"建表语句需要以 'CREATE TABLE IF NOT EXISTS' 开头: {sql}")

        l_idx = remaining_sql.find("(")
        if l_idx == -1:
            raise Exception(f"不合法的建表语句, 缺少 '(': {sql}")
        table_name = remaining_sql[:l_idx]
        table_name = self.get_real_name(table_name)
        table = Table(table_name, self.logger)

        r_idx = remaining_sql.rfind(")")
        if r_idx == -1:
            raise Exception(f"不合法的建表语句, 缺少 ')': {sql}")
        columns_define_sql = remaining_sql[l_idx + 1 : r_idx]
        table_define_sql = remaining_sql[r_idx + 1 :].strip(" ;")

        self.parse_sql_table_options(table_define_sql, table)

        columns_sqls = [line.strip(" ,\t") for line in columns_define_sql.splitlines() if line.strip(" ,\t")]
        for column_sql in columns_sqls:
            self.parse_sql_table_struct(column_sql, table)

        self.check_table(table)
        db.add_table(table)

    def parse_sql_table_options(self, sql: str, table: Table):
        if not sql:
            return
        remaining_sql = sql
        while remaining_sql != "":
            key, remaining_sql = next_token(remaining_sql)
            key = key.upper()
            if key != "":
                raise Exception(f"表定义中包含不合法的关键字 '{key}': {sql}")

    def get_real_name(self, name: str):
        real_name = name.strip(" `\n;")
        chars_to_check = {".", '"', "'"}
        if chars_to_check & set(real_name):
            raise Exception(f"名称中包含不合法字符 ('.', '\"', '''): {name}")
        return real_name

    def get_real_column_name(self, name: str):
        real_name = name
        idx = real_name.find("(")
        if idx != -1:
            real_name = real_name[:idx]

        real_name = real_name.strip(" `\n")
        chars_to_check = {".", '"', "'"}
        if chars_to_check & set(real_name):
            raise Exception(f"名称中包含不合法字符 ('.', '\"', '''): {name}")
        return real_name

    def parse_sql_table_struct(self, column_sql: str, table: Table):
        remaining_sql = column_sql
        first_token, remaining_sql = next_token(remaining_sql)
        if first_token == "PRIMARY":
            token, remaining_sql = next_token(remaining_sql)
            if token != "KEY":
                raise Exception(f"主键索引语法错误 :{column_sql}")
            if remaining_sql[0] != "(":
                raise Exception(f"主键索引语法错误 :{column_sql}")
            ridx = remaining_sql.rfind(")")
            if ridx == -1:
                raise Exception(f"主键索引语法错误 :{column_sql}")
            columns_str = remaining_sql[1:ridx]
            columns = [line.strip() for line in columns_str.split(",") if line.strip()]
            index = PrimaryIndex(table.TableName)
            for column in columns:
                column_name = self.get_real_column_name(column)
                index.add_column(column_name)
            table.set_primary_index(index)

        elif first_token == "UNIQUE":
            token, remaining_sql = next_token(remaining_sql)
            if token != "KEY" and token != "INDEX":
                raise Exception(f"唯一索引语法错误 :{column_sql}")
            index_name, remaining_sql = next_token(remaining_sql)
            if remaining_sql == "" or remaining_sql[0] != "(":
                raise Exception(f"唯一索引语法错误 :{column_sql}")
            ridx = remaining_sql.rfind(")")
            if ridx == -1:
                raise Exception(f"唯一索引语法错误 :{column_sql}")
            index_name = self.get_real_name(index_name)
            columns_str = remaining_sql[1:ridx]
            columns = [line.strip() for line in columns_str.split(",") if line.strip()]
            index = UniqueIndex(table.TableName, index_name, self.logger)
            for column in columns:
                column_name = self.get_real_column_name(column)
                index.add_column(column_name)
            table.add_index(index)

        elif first_token == "KEY" or first_token == "INDEX":
            index_name, remaining_sql = next_token(remaining_sql)
            if remaining_sql == "" or remaining_sql[0] != "(":
                raise Exception(f"索引语法错误 :{column_sql}")
            ridx = remaining_sql.rfind(")")
            if ridx == -1:
                raise Exception(f"索引语法错误 :{column_sql}")
            index_name = self.get_real_name(index_name)
            columns_str = remaining_sql[1:ridx]
            columns = [line.strip() for line in columns_str.split(",") if line.strip()]
            index = Index(table.TableName, index_name, self.logger)
            for column in columns:
                column_name = self.get_real_column_name(column)
                index.add_column(column_name)
            table.add_index(index)

        elif first_token == "CONSTRAINT":
            tokens, remaining_sql = next_tokens(remaining_sql, 3)
            if tokens[1] != "FOREIGN" or tokens[2] != "KEY":
                raise Exception(f"约束语法错误 :{column_sql}")
            table.add_foreign_key(column_sql)

        elif first_token == "FOREIGN":
            token, remaining_sql = next_token(remaining_sql)
            if token != "KEY":
                raise Exception(f"外键约束语法错误 :{column_sql}")
            table.add_foreign_key(column_sql)

        else:
            column_name = first_token
            column = self.parse_sql_column_define(column_name, remaining_sql)
            table.add_column(column)

    def parse_sql_column_define(self, column_name: str, column_sql: str):
        remaining_sql = column_sql
        column_name = self.get_real_column_name(column_name)
        column_type, remaining_sql = next_token(remaining_sql)
        column = Column(column_name, column_type)

        column.ColumnLen, remaining_sql = self.parse_sql_column_len(remaining_sql)
        column.ColumnUnsigned, remaining_sql = self.parse_sql_column_unsigned(remaining_sql)

        while remaining_sql != "":
            key, remaining_sql = next_token(remaining_sql)
            key = key.upper()
            if key == "COMMENT":
                column.ColumnComment, remaining_sql = next_token(remaining_sql)
            elif key == "NULL":
                column.ColumnNull = True
            elif key == "NOT":
                key2, remaining_sql = next_token(remaining_sql)
                if key2.upper() == "NULL":
                    column.ColumnNull = False
                else:
                    raise Exception(f"NOT NULL 语法错误 :{column_sql}")
            elif key == "DEFAULT":
                default_value, remaining_sql = next_token(remaining_sql)
                if remaining_sql.startswith("("):
                    stack = []
                    end_idx = 0
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

                    default_value += remaining_sql[: end_idx + 1]
                    remaining_sql = remaining_sql[end_idx + 1 :].strip(" ")
                column.ColumnDefault = default_value
            else:
                raise Exception(f"列定义中包含不合法的关键字 '{key}': {column_sql}")

        return column

    def parse_sql_column_len(self, column_sql: str):
        if column_sql.startswith("("):
            idx = column_sql.find(")")
            if idx != -1:
                remaining_sql = column_sql[idx + 1 :].strip(" ")
                column_len = column_sql[1:idx].strip(" ")
                return column_len, remaining_sql
        return None, column_sql

    def parse_sql_column_unsigned(self, column_sql: str):
        token, remaining_sql = next_token(column_sql)
        if token.upper().startswith("UNSIGNED"):
            return True, remaining_sql.strip(" ")
        return False, column_sql

    def parse_sql_create_unique_index(self, sql: str, db: Database):
        remaining_sql = sql
        tokens, remaining_sql = next_tokens(remaining_sql, 6)
        if (
            len(tokens) != 6
            or tokens[0].upper() != "CREATE"
            or tokens[1].upper() != "UNIQUE"
            or tokens[2].upper() != "INDEX"
            or tokens[3].upper() != "IF"
            or tokens[4].upper() != "NOT"
            or tokens[5].upper() != "EXISTS"
        ):
            raise Exception(f"唯一索引语法错误 :{sql}")

        index_name, remaining_sql = next_token(remaining_sql)
        index_name = self.get_real_name(index_name)

        token, remaining_sql = next_token(remaining_sql)
        if token != "ON":
            raise Exception(f"唯一索引语法错误 :{sql}")

        table_name, remaining_sql = next_token(remaining_sql)
        table_name = self.get_real_name(table_name)
        table = db.get_table(table_name)
        if not table:
            raise Exception(f"表不存在 :{table_name}")

        if remaining_sql[0] != "(":
            raise Exception(f"唯一索引语法错误 :{sql}")
        ridx = remaining_sql.rfind(")")
        if ridx == -1:
            raise Exception(f"唯一索引语法错误 :{sql}")
        columns_str = remaining_sql[1:ridx]
        columns = [line.strip() for line in columns_str.split(",") if line.strip()]
        index = UniqueIndex(table.TableName, index_name, self.logger)
        for column in columns:
            column_name = self.get_real_column_name(column)
            index.add_column(column_name)
        table.add_index(index)

    def parse_sql_create_index(self, sql: str, db: Database):
        remaining_sql = sql
        tokens, remaining_sql = next_tokens(remaining_sql, 5)
        if (
            len(tokens) != 5
            or tokens[0].upper() != "CREATE"
            or tokens[1].upper() != "INDEX"
            or tokens[2].upper() != "IF"
            or tokens[3].upper() != "NOT"
            or tokens[4].upper() != "EXISTS"
        ):
            raise Exception(f"普通索引语法错误 :{sql}")

        index_name, remaining_sql = next_token(remaining_sql)
        index_name = self.get_real_name(index_name)

        token, remaining_sql = next_token(remaining_sql)
        if token != "ON":
            raise Exception(f"普通索引语法错误 :{sql}")

        table_name, remaining_sql = next_token(remaining_sql)
        table_name = self.get_real_name(table_name)
        table = db.get_table(table_name)
        if not table:
            raise Exception(f"表不存在 :{table_name}")

        if remaining_sql[0] != "(":
            raise Exception(f"普通索引语法错误 :{sql}")
        ridx = remaining_sql.rfind(")")
        if ridx == -1:
            raise Exception(f"普通索引语法错误 :{sql}")
        columns_str = remaining_sql[1:ridx]
        columns = [line.strip() for line in columns_str.split(",") if line.strip()]
        index = Index(table.TableName, index_name, self.logger)
        for column in columns:
            column_name = self.get_real_column_name(column)
            index.add_column(column_name)
        table.add_index(index)

    def check_table(self, table: Table):
        if table.PrimaryIndex is None:
            if self.check_config.AllowNonePrimaryKey:
                if self.logger:
                    self.logger.warning(f"表 '{table.TableName}' 中缺少主键索引")
            else:
                raise Exception(f"表 '{table.TableName}' 中缺少主键索引")
        else:
            column_names = table.PrimaryIndex.Columns
            for column_name in column_names:
                if column_name not in table.Columns:
                    raise Exception(f"表 '{table.TableName}' 中不存在主键索引中的字段 '{column_name}'")

        for index_name in table.Indices:
            index = table.Indices[index_name]
            column_names = index.Columns
            for column_name in column_names:
                if column_name not in table.Columns:
                    raise Exception(f"表 '{table.TableName}' 中不存在索引 '{index_name}' 中的字段 '{column_name}'")

        for column_name in table.Columns:
            column = table.Columns[column_name]
            self.check_column(table.TableName, column)

        if len(table.ForeignKeys) > 0:
            if self.check_config.AllowForeignKey:
                if self.logger:
                    self.logger.warning(f"表 '{table.TableName}' 中存在外键约束")
            else:
                raise Exception(f"表 '{table.TableName}' 中存在外键约束, 但配置中不允许外键约束")

    def check_column(self, table_name: str, column: Column):
        if column.ColumnType in ("TEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "JSON"):
            if column.ColumnDefault is not None and column.ColumnDefault.upper() != "NULL":
                raise Exception(f"表 '{table_name}' 中字段 '{column.ColumnName}' 是文本类型字段, 不支持设置默认值")
