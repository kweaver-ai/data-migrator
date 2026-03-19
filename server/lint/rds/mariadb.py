#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 静态校验 — 继承 MariaDBParser，无 DB 依赖"""
from logging import Logger

from server.lint.rds.base import LintRDS
from server.db.dialect._parser.mariadb import MariaDBParser
from server.check.check_config import CheckConfig
from server.utils.table_define import Database, Table, Index, PrimaryIndex, UniqueIndex, Column
from server.utils.token import next_token, next_tokens


class LintMariaDB(MariaDBParser, LintRDS):
    def __init__(self, check_config: CheckConfig, logger: Logger):
        LintRDS.__init__(self, check_config, logger)

    # ── check_init / check_update ────────────────────────────────────────────

    def check_init(self, sql_list: list):
        if not sql_list:
            return
        sql = sql_list[0]
        token, remaining_sql = next_token(sql)
        if token.upper() != "USE":
            raise Exception(f"init文件中第一条语句必须为 'USE database': {sql}")
        db = self.parse_sql_use_db(sql)

        for sql in sql_list[1:]:
            token, remaining_sql = next_token(sql)
            token = token.upper()
            if token == "USE":
                db = self.parse_sql_use_db(sql)
            elif token == "CREATE":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 == "TABLE":
                    self._parse_and_check_create_table(sql, db)
                elif token2 == "UNIQUE":
                    self._parse_and_check_create_unique_index(sql, db)
                elif token2 == "INDEX":
                    self._parse_and_check_create_index(sql, db)
                elif token2 == "VIEW":
                    continue
                elif token2 == "OR":
                    tokens, _ = next_tokens(remaining_sql, 2)
                    if [t.upper() for t in tokens] != ["REPLACE", "VIEW"]:
                        raise Exception(f"不合法的sql语句, 仅支持 'CREATE OR REPLACE VIEW': {sql}")
                    continue
                else:
                    raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
            elif token == "INSERT":
                continue
            else:
                raise Exception(f"不合法的sql语句, 仅支持 'USE', 'CREATE TABLE', 'INSERT': {sql}")

    def check_update(self, sql_list: list):
        if not sql_list:
            return
        sql = sql_list[0]
        token, remaining_sql = next_token(sql)
        if token.upper() != "USE":
            raise Exception(f"升级文件中第一条语句必须为 'USE database': {sql}")
        db = self.parse_sql_use_db(sql)

        for sql in sql_list[1:]:
            token, remaining_sql = next_token(sql)
            token = token.upper()
            if token == "USE":
                db = self.parse_sql_use_db(sql)
            elif token == "CREATE":
                token2, remaining_sql = next_token(remaining_sql)
                token2 = token2.upper()
                if token2 == "TABLE":
                    self._parse_and_check_create_table(sql, db)
                elif token2 in ("UNIQUE", "INDEX"):
                    continue
                elif token2 == "VIEW":
                    continue
                elif token2 == "OR":
                    tokens, _ = next_tokens(remaining_sql, 2)
                    if [t.upper() for t in tokens] != ["REPLACE", "VIEW"]:
                        raise Exception(f"不合法的sql语句, 仅支持 'CREATE OR REPLACE VIEW': {sql}")
                    continue
                else:
                    raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
            elif token in ("INSERT", "UPDATE", "DELETE"):
                continue
            elif token == "DROP":
                token2, remaining_sql = next_token(remaining_sql)
                if token2.upper() not in ("INDEX", "TABLE", "VIEW"):
                    raise Exception(f"不合法的sql语句, 仅支持 'DROP INDEX/TABLE/VIEW': {sql}")
                continue
            elif token == "ALTER":
                token2, remaining_sql = next_token(remaining_sql)
                if token2.upper() != "TABLE":
                    raise Exception(f"不合法的sql语句, 仅支持 'ALTER TABLE': {sql}")
                continue
            elif token == "RENAME":
                continue
            else:
                raise Exception(f"不合法的sql语句: {sql}")

    # ── 建表解析与校验 ────────────────────────────────────────────────────────

    def _parse_and_check_create_table(self, sql: str, db: Database):
        _, remaining_sql = next_tokens(sql, 2)  # skip CREATE TABLE

        # skip optional IF NOT EXISTS
        token, _ = next_token(remaining_sql)
        if token.upper() == "IF":
            _, remaining_sql = next_tokens(remaining_sql, 3)  # skip IF NOT EXISTS

        table_name_tok, remaining_sql = next_token(remaining_sql)
        table_name = self.get_real_name(table_name_tok)
        table = Table(table_name, self.logger)
        # remaining_sql now starts with "(...)"

        r_idx = remaining_sql.rfind(")")
        if r_idx == -1:
            raise Exception(f"不合法的建表语句, 缺少 ')': {sql}")

        self._parse_table_options(remaining_sql[r_idx + 1:].strip(" ;"), table)

        for col_sql in remaining_sql[1:r_idx].splitlines():  # skip opening "("
            col_sql = col_sql.strip(" ,\t")
            if col_sql:
                self._parse_table_struct(col_sql, table)

        self._check_table(table)
        db.add_table(table)

    def _parse_table_options(self, sql: str, table: Table):
        if not sql:
            return
        remaining_sql = sql
        while remaining_sql:
            key, remaining_sql = next_token(remaining_sql)
            key_upper = key.upper()
            if key_upper == "DEFAULT":
                # DEFAULT CHARSET = utf8mb4  or  DEFAULT CHARACTER SET = utf8mb4
                next_tok, _ = next_token(remaining_sql)
                if next_tok.upper() in ("CHARSET", "COLLATE"):
                    _, remaining_sql = next_token(remaining_sql)  # skip CHARSET/COLLATE
                    _, remaining_sql = next_token(remaining_sql)  # skip value
                elif next_tok.upper() == "CHARACTER":
                    _, remaining_sql = next_token(remaining_sql)  # skip CHARACTER
                    _, remaining_sql = next_token(remaining_sql)  # skip SET
                    _, remaining_sql = next_token(remaining_sql)  # skip value
                else:
                    _, remaining_sql = next_token(remaining_sql)  # consume value
            elif key_upper in ("ENGINE", "AUTO_INCREMENT", "CHARSET",
                               "CHARACTER", "COLLATE", "COMMENT", "ROW_FORMAT",
                               "AVG_ROW_LENGTH", "MAX_ROWS", "MIN_ROWS",
                               "PACK_KEYS", "CHECKSUM", "DELAY_KEY_WRITE",
                               "CONNECTION", "DATA", "INDEX"):
                # consume value token
                _, remaining_sql = next_token(remaining_sql)
            elif key_upper == "SET":
                _, remaining_sql = next_token(remaining_sql)
                _, remaining_sql = next_token(remaining_sql)
            elif key_upper == "=":
                continue
            elif key:
                raise Exception(f"表定义中包含不合法的关键字 '{key}': {sql}")

    def _parse_table_struct(self, column_sql: str, table: Table):
        first_token, remaining_sql = next_token(column_sql)
        if first_token.upper() == "PRIMARY":
            token, remaining_sql = next_token(remaining_sql)
            if token.upper() != "KEY":
                raise Exception(f"主键索引语法错误: {column_sql}")
            ridx = remaining_sql.rfind(")")
            index = PrimaryIndex(table.TableName)
            for col in remaining_sql[1:ridx].split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.set_primary_index(index)
        elif first_token.upper() == "UNIQUE":
            token, remaining_sql = next_token(remaining_sql)
            if token.upper() not in ("KEY", "INDEX"):
                raise Exception(f"唯一索引语法错误: {column_sql}")
            index_name, remaining_sql = next_token(remaining_sql)
            ridx = remaining_sql.rfind(")")
            index = UniqueIndex(table.TableName, self.get_real_name(index_name), self.logger)
            for col in remaining_sql[1:ridx].split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.add_index(index)
        elif first_token.upper() in ("KEY", "INDEX"):
            index_name, remaining_sql = next_token(remaining_sql)
            ridx = remaining_sql.rfind(")")
            index = Index(table.TableName, self.get_real_name(index_name), self.logger)
            for col in remaining_sql[1:ridx].split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.add_index(index)
        elif first_token.upper() == "CONSTRAINT":
            tokens, _ = next_tokens(remaining_sql, 3)
            if tokens[1].upper() != "FOREIGN" or tokens[2].upper() != "KEY":
                raise Exception(f"约束语法错误: {column_sql}")
            table.add_foreign_key(column_sql)
        elif first_token.upper() == "FOREIGN":
            token, _ = next_token(remaining_sql)
            if token.upper() != "KEY":
                raise Exception(f"外键约束语法错误: {column_sql}")
            table.add_foreign_key(column_sql)
        else:
            column = self.parse_sql_column_define(first_token, remaining_sql)
            table.add_column(column)

    def _parse_and_check_create_unique_index(self, sql: str, db: Database):
        tokens, remaining_sql = next_tokens(sql, 6)
        if (len(tokens) != 6 or tokens[0].upper() != "CREATE" or tokens[1].upper() != "UNIQUE"
                or tokens[2].upper() != "INDEX" or tokens[3].upper() != "IF"
                or tokens[4].upper() != "NOT" or tokens[5].upper() != "EXISTS"):
            raise Exception(f"唯一索引语法错误: {sql}")
        index_name, remaining_sql = next_token(remaining_sql)
        index_name = self.get_real_name(index_name)
        token, remaining_sql = next_token(remaining_sql)
        if token.upper() != "ON":
            raise Exception(f"唯一索引语法错误: {sql}")
        table_name, remaining_sql = next_token(remaining_sql)
        table_name = self.get_real_name(table_name)
        table = db.get_table(table_name)
        if not table:
            raise Exception(f"表不存在: {table_name}")
        ridx = remaining_sql.rfind(")")
        index = UniqueIndex(table.TableName, index_name, self.logger)
        for col in remaining_sql[1:ridx].split(","):
            index.add_column(self.get_real_column_name(col.strip()))
        table.add_index(index)

    def _parse_and_check_create_index(self, sql: str, db: Database):
        tokens, remaining_sql = next_tokens(sql, 5)
        if (len(tokens) != 5 or tokens[0].upper() != "CREATE" or tokens[1].upper() != "INDEX"
                or tokens[2].upper() != "IF" or tokens[3].upper() != "NOT" or tokens[4].upper() != "EXISTS"):
            raise Exception(f"普通索引语法错误: {sql}")
        index_name, remaining_sql = next_token(remaining_sql)
        index_name = self.get_real_name(index_name)
        token, remaining_sql = next_token(remaining_sql)
        if token.upper() != "ON":
            raise Exception(f"普通索引语法错误: {sql}")
        table_name, remaining_sql = next_token(remaining_sql)
        table_name = self.get_real_name(table_name)
        table = db.get_table(table_name)
        if not table:
            raise Exception(f"表不存在: {table_name}")
        ridx = remaining_sql.rfind(")")
        index = Index(table.TableName, index_name, self.logger)
        for col in remaining_sql[1:ridx].split(","):
            index.add_column(self.get_real_column_name(col.strip()))
        table.add_index(index)

    def _check_table(self, table: Table):
        if table.PrimaryIndex is None:
            if self.check_config.AllowNonePrimaryKey:
                self.logger.warning(f"表 '{table.TableName}' 中缺少主键索引")
            else:
                raise Exception(f"表 '{table.TableName}' 中缺少主键索引")
        else:
            for col_name in table.PrimaryIndex.Columns:
                if col_name not in table.Columns:
                    raise Exception(f"表 '{table.TableName}' 主键中字段 '{col_name}' 不存在")

        for idx_name, index in table.Indices.items():
            for col_name in index.Columns:
                if col_name not in table.Columns:
                    raise Exception(f"表 '{table.TableName}' 索引 '{idx_name}' 中字段 '{col_name}' 不存在")

        for col_name, column in table.Columns.items():
            self.check_column(table.TableName, column)

        if table.ForeignKeys:
            if self.check_config.AllowForeignKey:
                self.logger.warning(f"表 '{table.TableName}' 中存在外键约束")
            else:
                raise Exception(f"表 '{table.TableName}' 中存在外键约束, 但配置中不允许外键约束")

    def check_column(self, table_name: str, column: Column):
        if column.ColumnType in ("TEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "JSON"):
            if column.ColumnDefault is not None and column.ColumnDefault.upper() != "NULL":
                raise Exception(f"表 '{table_name}' 字段 '{column.ColumnName}' 文本类型不支持默认值")
