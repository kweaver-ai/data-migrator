#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""幂等预检 - 基于 sqlparse 的 DDL 类型检测 + INFORMATION_SCHEMA 查询 + 跳过/执行

支持的幂等检测类型：
  - CREATE TABLE -> 检查表是否已存在
  - ALTER TABLE ADD COLUMN -> 检查列是否已存在
  - CREATE INDEX / CREATE UNIQUE INDEX -> 检查索引是否已存在
  - ALTER TABLE ADD CONSTRAINT -> 检查约束是否已存在
  - 无法识别的 DDL -> 直接执行
"""
from logging import Logger

import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import Keyword, DDL, Name, Punctuation

from server.db.operate import OperateDB
from server.db.dialect.base import RDSDialect


def _strip_backticks(name: str) -> str:
    """去除反引号和双引号"""
    return name.strip("`\"")


def _extract_name_parts(identifier: Identifier) -> tuple:
    """从 Identifier 中提取 (schema, name)。

    支持格式: `db`.`name`, db.name, name
    返回: (schema_or_none, object_name)
    """
    tokens = [t for t in identifier.tokens
              if t.ttype not in (Punctuation,) and not t.is_whitespace]
    if len(tokens) >= 2:
        return _strip_backticks(tokens[0].value), _strip_backticks(tokens[-1].value)
    return None, _strip_backticks(identifier.get_name() or identifier.value)


def _get_significant_tokens(parsed):
    """获取语句中所有有意义的 token（跳过空白和注释）"""
    return [t for t in parsed.flatten()
            if not t.is_whitespace and t.ttype not in (sqlparse.tokens.Comment.Single,
                                                        sqlparse.tokens.Comment.Multiline,
                                                        sqlparse.tokens.Newline)]


class IdempotencyChecker:
    def __init__(self, operate_db: OperateDB, dialect: RDSDialect, logger: Logger):
        self.db = operate_db
        self.dialect = dialect
        self.logger = logger

    def should_skip(self, sql: str) -> bool:
        """
        判断 SQL 是否应该跳过执行（已存在则跳过）。
        返回 True 表示跳过，False 表示需要执行。
        """
        parsed = sqlparse.parse(sql.strip())
        if not parsed:
            return False

        stmt = parsed[0]
        stmt_type = stmt.get_type()

        if stmt_type == "CREATE":
            return self._check_create(stmt)
        elif stmt_type == "ALTER":
            return self._check_alter(stmt)

        return False

    def _check_create(self, stmt) -> bool:
        """处理 CREATE TABLE / CREATE INDEX / CREATE UNIQUE INDEX"""
        tokens = _get_significant_tokens(stmt)
        upper_values = [t.value.upper() for t in tokens]

        # 判断是 CREATE TABLE 还是 CREATE [UNIQUE] INDEX
        if "TABLE" in upper_values:
            return self._check_create_table(stmt)
        elif "INDEX" in upper_values:
            return self._check_create_index(stmt)

        return False

    def _check_create_table(self, stmt) -> bool:
        """CREATE TABLE [IF NOT EXISTS] [db.]table_name"""
        # 从 statement 中找 Identifier（表名）
        for token in stmt.tokens:
            if isinstance(token, Identifier):
                db_name, table_name = _extract_name_parts(token)
                if db_name and table_name:
                    return self._table_exists(db_name, table_name)
                break

        # fallback: 从 flatten tokens 中按位置提取
        tokens = _get_significant_tokens(stmt)
        upper_values = [t.value.upper() for t in tokens]

        try:
            idx = upper_values.index("TABLE")
            # 跳过可能的 IF NOT EXISTS
            pos = idx + 1
            while pos < len(tokens) and upper_values[pos] in ("IF", "NOT", "EXISTS"):
                pos += 1
            if pos < len(tokens):
                name = _strip_backticks(tokens[pos].value)
                # 检查是否有 schema.table 格式
                if pos + 2 < len(tokens) and tokens[pos + 1].value == ".":
                    db_name = name
                    table_name = _strip_backticks(tokens[pos + 2].value)
                    return self._table_exists(db_name, table_name)
        except (ValueError, IndexError):
            pass

        return False

    def _check_create_index(self, stmt) -> bool:
        """CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name ON [db.]table_name"""
        tokens = _get_significant_tokens(stmt)
        upper_values = [t.value.upper() for t in tokens]

        if not self.dialect.QUERY_INDEX_SQL:
            return False

        try:
            idx = upper_values.index("INDEX")
            # 跳过 IF NOT EXISTS
            pos = idx + 1
            while pos < len(tokens) and upper_values[pos] in ("IF", "NOT", "EXISTS"):
                pos += 1

            if pos >= len(tokens):
                return False
            index_name = _strip_backticks(tokens[pos].value)

            # 找 ON 关键字
            on_idx = upper_values.index("ON", pos)
            tbl_pos = on_idx + 1
            if tbl_pos >= len(tokens):
                return False

            tbl_name = _strip_backticks(tokens[tbl_pos].value)
            db_name = None

            if tbl_pos + 2 < len(tokens) and tokens[tbl_pos + 1].value == ".":
                db_name = tbl_name
                tbl_name = _strip_backticks(tokens[tbl_pos + 2].value)

            if db_name and tbl_name:
                return self._index_exists(db_name, tbl_name, index_name)
        except (ValueError, IndexError):
            pass

        return False

    def _check_alter(self, stmt) -> bool:
        """处理 ALTER TABLE ... ADD COLUMN / ADD CONSTRAINT"""
        tokens = _get_significant_tokens(stmt)
        upper_values = [t.value.upper() for t in tokens]

        if "TABLE" not in upper_values:
            return False

        try:
            tbl_idx = upper_values.index("TABLE")
            tbl_pos = tbl_idx + 1
            if tbl_pos >= len(tokens):
                return False

            tbl_name = _strip_backticks(tokens[tbl_pos].value)
            db_name = None

            if tbl_pos + 2 < len(tokens) and tokens[tbl_pos + 1].value == ".":
                db_name = tbl_name
                tbl_name = _strip_backticks(tokens[tbl_pos + 2].value)

            # 找 ADD 关键字
            add_idx = None
            for i, v in enumerate(upper_values):
                if v == "ADD" and i > tbl_idx:
                    add_idx = i
                    break

            if add_idx is None:
                return False

            next_pos = add_idx + 1
            if next_pos >= len(tokens):
                return False

            next_kw = upper_values[next_pos]

            # ADD CONSTRAINT
            if next_kw == "CONSTRAINT":
                constraint_pos = next_pos + 1
                if constraint_pos < len(tokens) and db_name and self.dialect.QUERY_CONSTRAINT_SQL:
                    constraint_name = _strip_backticks(tokens[constraint_pos].value)
                    return self._constraint_exists(db_name, tbl_name, constraint_name)

            # ADD [COLUMN] [IF NOT EXISTS] column_name
            col_pos = next_pos
            if next_kw == "COLUMN":
                col_pos += 1
            # 跳过 IF NOT EXISTS
            while col_pos < len(tokens) and upper_values[col_pos] in ("IF", "NOT", "EXISTS"):
                col_pos += 1
            if col_pos < len(tokens) and db_name:
                column_name = _strip_backticks(tokens[col_pos].value)
                return self._column_exists(db_name, tbl_name, column_name)

        except (ValueError, IndexError):
            pass

        return False

    def _table_exists(self, db_name: str, table_name: str) -> bool:
        sql = self.dialect.QUERY_TABLE_SQL.format(db_name=db_name, table_name=table_name)
        result = self.db.fetch_all(sql)
        exists = len(result) > 0
        if exists:
            self.logger.info(f"[幂等跳过] 表已存在: {db_name}.{table_name}")
        return exists

    def _column_exists(self, db_name: str, table_name: str, column_name: str) -> bool:
        sql = self.dialect.QUERY_COLUMN_SQL.format(
            db_name=db_name, table_name=table_name, column_name=column_name
        )
        result = self.db.fetch_all(sql)
        exists = len(result) > 0
        if exists:
            self.logger.info(f"[幂等跳过] 列已存在: {db_name}.{table_name}.{column_name}")
        return exists

    def _index_exists(self, db_name: str, table_name: str, index_name: str) -> bool:
        sql = self.dialect.QUERY_INDEX_SQL.format(
            db_name=db_name, table_name=table_name, index_name=index_name
        )
        result = self.db.fetch_all(sql)
        exists = len(result) > 0
        if exists:
            self.logger.info(f"[幂等跳过] 索引已存在: {db_name}.{table_name}.{index_name}")
        return exists

    def _constraint_exists(self, db_name: str, table_name: str, constraint_name: str) -> bool:
        sql = self.dialect.QUERY_CONSTRAINT_SQL.format(
            db_name=db_name, table_name=table_name, constraint_name=constraint_name
        )
        result = self.db.fetch_all(sql)
        exists = len(result) > 0
        if exists:
            self.logger.info(f"[幂等跳过] 约束已存在: {db_name}.{table_name}.{constraint_name}")
        return exists
