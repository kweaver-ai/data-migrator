#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""JSON 升级文件执行器

解析 JSON 升级文件，对每条操作先查询是否已存在（幂等），再执行对应方言 SQL。
逻辑对齐 server-old/src/db/type/rds.py 的各操作方法。
"""
import json
from logging import Logger

from server.db.operate import OperateDB
from server.db.dialect.base import RDSDialect


class JsonExecutor:
    def __init__(self, operate_db: OperateDB, dialect: RDSDialect, logger: Logger):
        self.db = operate_db
        self.dialect = dialect
        self.logger = logger

    def execute(self, json_path: str):
        """执行一个 JSON 升级文件"""
        with open(json_path, "r", encoding="utf-8") as f:
            items = json.load(f)

        for item in items:
            db_name = item["db_name"]
            table_name = item["table_name"]
            obj_type = item["object_type"].upper()
            op = item["operation_type"].upper()
            name = item["object_name"]
            new_name = item.get("new_name", "")
            prop = item.get("object_property", "")
            comment = item.get("object_comment", "")

            self.logger.info(f"  JSON 操作: {op} {obj_type} {db_name}.{table_name}.{name}")

            if obj_type == "COLUMN":
                self._exec_column(db_name, table_name, op, name, new_name, prop, comment)
            elif obj_type in ("INDEX", "UNIQUE INDEX"):
                self._exec_index(db_name, table_name, obj_type, op, name, new_name, prop, comment)
            elif obj_type == "CONSTRAINT":
                self._exec_constraint(db_name, table_name, op, name, new_name, prop)
            elif obj_type == "TABLE":
                self._exec_table(db_name, table_name, op, new_name)
            elif obj_type == "DB":
                self._exec_db(db_name, op)
            else:
                raise Exception(f"不支持的 object_type '{obj_type}': {item}")

    # ── COLUMN ──────────────────────────────────────────────────────────────

    def _exec_column(self, db, table, op, name, new_name, prop, comment):
        if op == "ADD":
            if self.dialect.ADD_COLUMN_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 ADD COLUMN")
            exists = self._column_exists(db, table, name)
            if not exists:
                sql = self.dialect.ADD_COLUMN_SQL.format(
                    db_name=db, table_name=table,
                    column_name=name, column_property=prop, column_comment=comment,
                )
                self.db.run_ddl([sql])

        elif op == "MODIFY":
            if self.dialect.MODIFY_COLUMN_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 MODIFY COLUMN")
            exists = self._column_exists(db, table, name)
            if exists:
                sql = self.dialect.MODIFY_COLUMN_SQL.format(
                    db_name=db, table_name=table,
                    column_name=name, column_property=prop, column_comment=comment,
                )
                self.db.run_ddl([sql])

        elif op == "RENAME":
            if not new_name:
                raise Exception(f"RENAME COLUMN 缺少 new_name: {db}.{table}.{name}")
            if self.dialect.RENAME_COLUMN_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 RENAME COLUMN")
            exists = self._column_exists(db, table, name)
            if exists:
                sql = self.dialect.RENAME_COLUMN_SQL.format(
                    db_name=db, table_name=table,
                    column_name=name, new_name=new_name,
                    column_property=prop, column_comment=comment,
                )
                self.db.run_ddl([sql])

        elif op == "DROP":
            if self.dialect.DROP_COLUMN_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 DROP COLUMN")
            exists = self._column_exists(db, table, name)
            if exists:
                sql = self.dialect.DROP_COLUMN_SQL.format(
                    db_name=db, table_name=table, column_name=name,
                )
                self.db.run_ddl([sql])

        else:
            raise Exception(f"不支持的 operation_type '{op}' for COLUMN")

    # ── INDEX ────────────────────────────────────────────────────────────────

    def _exec_index(self, db, table, obj_type, op, name, new_name, prop, comment):
        if op == "ADD":
            if self.dialect.ADD_INDEX_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 ADD INDEX")
            exists = self._index_exists(db, table, name)
            if not exists:
                sql = self.dialect.ADD_INDEX_SQL.format(
                    db_name=db, table_name=table,
                    index_type=obj_type, index_name=name,
                    index_property=prop, index_comment=comment,
                )
                self.db.run_ddl([sql])

        elif op == "RENAME":
            if not new_name:
                raise Exception(f"RENAME INDEX 缺少 new_name: {db}.{table}.{name}")
            if self.dialect.RENAME_INDEX_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 RENAME INDEX")
            exists = self._index_exists(db, table, name)
            if exists:
                sql = self.dialect.RENAME_INDEX_SQL.format(
                    db_name=db, table_name=table,
                    index_name=name, new_name=new_name,
                )
                self.db.run_ddl([sql])

        elif op == "DROP":
            if self.dialect.DROP_INDEX_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 DROP INDEX")
            exists = self._index_exists(db, table, name)
            if exists:
                sql = self.dialect.DROP_INDEX_SQL.format(
                    db_name=db, table_name=table, index_name=name,
                )
                self.db.run_ddl([sql])

        else:
            raise Exception(f"不支持的 operation_type '{op}' for INDEX")

    # ── CONSTRAINT ───────────────────────────────────────────────────────────

    def _exec_constraint(self, db, table, op, name, new_name, prop):
        if op == "ADD":
            if self.dialect.ADD_CONSTRAINT_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 ADD CONSTRAINT")
            exists = self._constraint_exists(db, table, name)
            if not exists:
                sql = self.dialect.ADD_CONSTRAINT_SQL.format(
                    db_name=db, table_name=table,
                    constraint_name=name, constraint_property=prop,
                )
                self.db.run_ddl([sql])

        elif op == "RENAME":
            if not new_name:
                raise Exception(f"RENAME CONSTRAINT 缺少 new_name: {db}.{table}.{name}")
            if self.dialect.RENAME_CONSTRAINT_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 RENAME CONSTRAINT")
            exists = self._constraint_exists(db, table, name)
            if exists:
                sql = self.dialect.RENAME_CONSTRAINT_SQL.format(
                    db_name=db, table_name=table,
                    constraint_name=name, new_name=new_name,
                )
                self.db.run_ddl([sql])

        elif op == "DROP":
            if self.dialect.DROP_CONSTRAINT_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 DROP CONSTRAINT")
            exists = self._constraint_exists(db, table, name)
            if exists:
                sql = self.dialect.DROP_CONSTRAINT_SQL.format(
                    db_name=db, table_name=table, constraint_name=name,
                )
                self.db.run_ddl([sql])

        else:
            raise Exception(f"不支持的 operation_type '{op}' for CONSTRAINT")

    # ── TABLE ────────────────────────────────────────────────────────────────

    def _exec_table(self, db, table, op, new_name):
        if op == "RENAME":
            if not new_name:
                raise Exception(f"RENAME TABLE 缺少 new_name: {db}.{table}")
            if self.dialect.RENAME_TABLE_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 RENAME TABLE")
            exists = self._table_exists(db, table)
            if exists:
                sql = self.dialect.RENAME_TABLE_SQL.format(
                    db_name=db, table_name=table, new_name=new_name,
                )
                self.db.run_ddl([sql])

        elif op == "DROP":
            if self.dialect.DROP_TABLE_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 DROP TABLE")
            exists = self._table_exists(db, table)
            if exists:
                sql = self.dialect.DROP_TABLE_SQL.format(db_name=db, table_name=table)
                self.db.run_ddl([sql])

        else:
            raise Exception(f"不支持的 operation_type '{op}' for TABLE")

    # ── DB ───────────────────────────────────────────────────────────────────

    def _exec_db(self, db, op):
        if op == "DROP":
            if self.dialect.DROP_DATABASE_SQL is None:
                raise Exception(f"{self.dialect.DB_TYPE} 不支持 DROP DB")
            sql = self.dialect.DROP_DATABASE_SQL.format(db_name=db)
            self.db.run_ddl([sql])
        else:
            raise Exception(f"不支持的 operation_type '{op}' for DB")

    # ── 存在性查询 ────────────────────────────────────────────────────────────

    def _table_exists(self, db, table) -> bool:
        sql = self.dialect.QUERY_TABLE_SQL.format(db_name=db, table_name=table)
        return len(self.db.fetch_all(sql)) > 0

    def _column_exists(self, db, table, column) -> bool:
        sql = self.dialect.QUERY_COLUMN_SQL.format(
            db_name=db, table_name=table, column_name=column,
        )
        return len(self.db.fetch_all(sql)) > 0

    def _index_exists(self, db, table, index) -> bool:
        if self.dialect.QUERY_INDEX_SQL is None:
            return False  # 无法查询，直接执行（由数据库报错兜底）
        sql = self.dialect.QUERY_INDEX_SQL.format(
            db_name=db, table_name=table, index_name=index,
        )
        return len(self.db.fetch_all(sql)) > 0

    def _constraint_exists(self, db, table, constraint) -> bool:
        if self.dialect.QUERY_CONSTRAINT_SQL is None:
            return False
        sql = self.dialect.QUERY_CONSTRAINT_SQL.format(
            db_name=db, table_name=table, constraint_name=constraint,
        )
        return len(self.db.fetch_all(sql)) > 0
