#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 校验实现 - 继承 MariaDBDialect，添加 check-specific 校验逻辑"""
from logging import Logger

from server.check.rds.base import CheckRDS, load_rds_config
from server.check.check_config import CheckConfig
from server.db.dialect.mariadb import MariaDBDialect
from server.utils.table_define import Database, Table, Index, PrimaryIndex, UniqueIndex, Column
from server.utils.token import next_token, next_tokens


class CheckMariaDB(MariaDBDialect, CheckRDS):
    def __init__(self, check_config: CheckConfig, logger: Logger = None, is_primary: bool = True):
        rds_cfg = load_rds_config()["mariadb"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        conn_config = {**section, "DB_TYPE": "MARIADB"}
        MariaDBDialect.__init__(self, conn_config, logger)
        self.check_config = check_config

    # ── check_init / check_update ────────────────────────────────────────────

    def check_init(self, sql_list: list):
        if not sql_list:
            return
        sql = sql_list[0]
        token, remaining_sql = next_token(sql)
        if token.upper() != "USE":
            raise Exception(f"init文件中第一条语句必须为 'USE': {sql}")
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
                elif token2 in ("VIEW", "OR"):
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
            raise Exception(f"升级文件中第一条语句必须为 'USE': {sql}")
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
                elif token2 in ("VIEW", "OR"):
                    continue
                else:
                    raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
            elif token in ("INSERT", "UPDATE", "ALTER"):
                continue
            elif token == "DROP":
                token2, remaining_sql = next_token(remaining_sql)
                if token2.upper() not in ("INDEX", "TABLE", "VIEW"):
                    raise Exception(f"不合法的sql语句, 仅支持 'DROP INDEX/TABLE/VIEW': {sql}")
                continue
            elif token == "RENAME":
                token2, remaining_sql = next_token(remaining_sql)
                if token2.upper() != "TABLE":
                    raise Exception(f"不合法的sql语句, 仅支持 'RENAME TABLE': {sql}")
                continue
            else:
                raise Exception(f"不合法的sql语句: {sql}")

    # ── 建表解析与校验 ────────────────────────────────────────────────────────

    def _parse_and_check_create_table(self, sql: str, db: Database):
        tokens, remaining_sql = next_tokens(sql, 5)
        if (len(tokens) != 5 or tokens[0].upper() != "CREATE" or tokens[1].upper() != "TABLE"
                or tokens[2].upper() != "IF" or tokens[3].upper() != "NOT" or tokens[4].upper() != "EXISTS"):
            raise Exception(f"建表语句需要以 'CREATE TABLE IF NOT EXISTS' 开头: {sql}")

        l_idx = remaining_sql.find("(")
        if l_idx == -1:
            raise Exception(f"不合法的建表语句, 缺少 '(': {sql}")
        table_name = self.get_real_name(remaining_sql[:l_idx])
        table = Table(table_name, self.logger)

        r_idx = remaining_sql.rfind(")")
        if r_idx == -1:
            raise Exception(f"不合法的建表语句, 缺少 ')': {sql}")

        self._parse_table_options(remaining_sql[r_idx + 1:].strip(" ;"), table)

        for col_sql in remaining_sql[l_idx + 1:r_idx].splitlines():
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
            key = key.upper()
            if key in ("ENGINE", "AUTO_INCREMENT", "COLLATE", "COMMENT"):
                _, remaining_sql = next_token(remaining_sql)
                table.set_options(key, _)
            elif key == "DEFAULT":
                key2, remaining_sql = next_token(remaining_sql)
                if key2.upper() == "CHARSET":
                    value, remaining_sql = next_token(remaining_sql)
                    table.set_options("DEFAULT CHARSET", value)
                else:
                    raise Exception(f"表定义中包含不合法的关键字 '{key2}': {sql}")
            elif key == "":
                break
            else:
                raise Exception(f"表定义中包含不合法的关键字 '{key}': {sql}")

    def _parse_table_struct(self, column_sql: str, table: Table):
        first_token, remaining_sql = next_token(column_sql)
        if first_token == "PRIMARY":
            token, remaining_sql = next_token(remaining_sql)
            if token != "KEY":
                raise Exception(f"主键索引语法错误: {column_sql}")
            ridx = remaining_sql.rfind(")")
            columns_str = remaining_sql[1:ridx]
            index = PrimaryIndex(table.TableName)
            for col in columns_str.split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.set_primary_index(index)
        elif first_token == "UNIQUE":
            token, remaining_sql = next_token(remaining_sql)
            if token not in ("KEY", "INDEX"):
                raise Exception(f"唯一索引语法错误: {column_sql}")
            index_name, remaining_sql = next_token(remaining_sql)
            ridx = remaining_sql.rfind(")")
            index = UniqueIndex(table.TableName, self.get_real_name(index_name), self.logger)
            for col in remaining_sql[1:ridx].split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.add_index(index)
        elif first_token in ("KEY", "INDEX"):
            index_name, remaining_sql = next_token(remaining_sql)
            ridx = remaining_sql.rfind(")")
            index = Index(table.TableName, self.get_real_name(index_name), self.logger)
            for col in remaining_sql[1:ridx].split(","):
                index.add_column(self.get_real_column_name(col.strip()))
            table.add_index(index)
        elif first_token == "CONSTRAINT":
            tokens, _ = next_tokens(remaining_sql, 3)
            if (tokens[0] == "FOREIGN" and tokens[1] == "KEY") or (tokens[1] == "FOREIGN" and tokens[2] == "KEY"):
                table.add_foreign_key(column_sql)
            else:
                raise Exception(f"约束语法错误: {column_sql}")
        elif first_token == "FOREIGN":
            token, _ = next_token(remaining_sql)
            if token != "KEY":
                raise Exception(f"外键约束语法错误: {column_sql}")
            table.add_foreign_key(column_sql)
        else:
            column = self.parse_sql_column_define(first_token, remaining_sql)
            table.add_column(column)

    def _check_table(self, table: Table):
        if table.PrimaryIndex is None:
            if self.check_config.AllowNonePrimaryKey:
                if self.logger:
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
                if self.logger:
                    self.logger.warning(f"表 '{table.TableName}' 中存在外键约束")
            else:
                raise Exception(f"表 '{table.TableName}' 中存在外键约束, 但配置中不允许外键约束")

    def check_column(self, table_name: str, column: Column):
        if column.ColumnType in ("TEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "JSON"):
            if column.ColumnDefault is not None and column.ColumnDefault.upper() != "NULL":
                raise Exception(f"表 '{table_name}' 字段 '{column.ColumnName}' 文本类型不支持默认值")
        elif column.ColumnType == "TINYINT" and column.ColumnLen == "1":
            raise Exception(f"表 '{table_name}' 字段 '{column.ColumnName}' 不支持 tinyint(1)，请使用 BOOLEAN")
