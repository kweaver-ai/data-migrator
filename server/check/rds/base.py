#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDS 校验抽象基类 - 复用 tools/rds/rds.py"""
from abc import ABC, abstractmethod
import os
from logging import Logger
from pathlib import Path

import yaml

try:
    import rdsdriver
except ImportError:
    rdsdriver = None

from server.check.check_config import CheckConfig
from server.utils.token import next_token, next_tokens

# 默认配置文件路径（可通过环境变量 CHECK_RDS_CONFIG 覆盖）
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "check_rds_config.yaml"


def load_rds_config() -> dict:
    """加载 RDS 校验连接配置"""
    config_path = os.environ.get("CHECK_RDS_CONFIG", str(_DEFAULT_CONFIG_PATH))
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_rds_config(rds_cfg: dict, required_db_types: list):
    """校验 check_rds_config.yaml 是否包含所有需要的数据库类型，缺失时提前报错"""
    missing = [t for t in required_db_types if t not in rds_cfg]
    if missing:
        config_path = os.environ.get("CHECK_RDS_CONFIG", str(_DEFAULT_CONFIG_PATH))
        raise Exception(
            f"check_rds_config.yaml 缺少以下数据库类型的连接配置: {missing}，"
            f"配置文件路径: {config_path}"
        )


class CheckRDS(ABC):
    """RDS 检查接口类"""

    def __init__(self, check_config: CheckConfig, logger: Logger = None):
        self.check_config = check_config
        self.logger = logger

    @abstractmethod
    def check_init(self, sql_list: list):
        pass

    @abstractmethod
    def check_update(self, sql_list: list):
        pass

    @abstractmethod
    def get_column_type(self, column: dict) -> tuple:
        pass

    @abstractmethod
    def parse_sql_column_define(self, column_name: str, remaining_sql: str):
        """解析字段定义 SQL"""
        pass

    @abstractmethod
    def check_column(self, table_name: str, column):
        """检查字段定义是否合法"""
        pass

    @abstractmethod
    def parse_sql_use_db(self, sql: str):
        """解析 USE / SET SCHEMA 等切库语句，返回 Database 对象"""
        pass

    @abstractmethod
    def get_real_name(self, name: str):
        """去除名称中的引号和空白字符"""
        pass

    # ── 数据库操作方法（供 schema_checker JSON 执行使用）──

    def add_column(self, db_name: str, table_name: str, column_name: str, column_property: str, column_comment: str):
        """新增字段"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) == 0:
                        sql = self.ADD_COLUMN_SQL.format(
                            db_name=db_name, table_name=table_name, column_name=column_name,
                            column_property=column_property, column_comment=column_comment
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"add_column: {db_name}.{table_name}.{column_name} 失败, 错误: {e}") from e

    def modify_column(self, db_name: str, table_name: str, column_name: str, column_property: str, column_comment: str):
        """修改字段"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) > 0:
                        sql = self.MODIFY_COLUMN_SQL.format(
                            db_name=db_name, table_name=table_name, column_name=column_name,
                            column_property=column_property, column_comment=column_comment
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"modify_column: {db_name}.{table_name}.{column_name} 失败, 错误: {e}") from e

    def rename_column(self, db_name: str, table_name: str, column_name: str, new_name: str, column_property: str, column_comment: str):
        """重命名列"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) > 0:
                        sql = self.RENAME_COLUMN_SQL.format(
                            db_name=db_name, table_name=table_name, column_name=column_name,
                            new_name=new_name, column_property=column_property, column_comment=column_comment
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"rename_column: {db_name}.{table_name}.{column_name} 失败, 错误: {e}") from e

    def drop_column(self, db_name: str, table_name: str, column_name: str):
        """删除列"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) > 0:
                        sql = self.DROP_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"drop_column: {db_name}.{table_name}.{column_name} 失败, 错误: {e}") from e

    def add_index(self, db_name: str, table_name: str, index_type: str, index_name: str, index_property: str, index_comment: str):
        """新增索引"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = False
                    if self.QUERY_INDEX_SQL is not None:
                        sql = self.QUERY_INDEX_SQL.format(db_name=db_name, table_name=table_name, index_name=index_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) > 0:
                            exist = True
                    if not exist:
                        sql = self.ADD_INDEX_SQL.format(
                            db_name=db_name, table_name=table_name, index_type=index_type,
                            index_name=index_name, index_property=index_property, index_comment=index_comment
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"add_index: {db_name}.{table_name}.{index_name} 失败, 错误: {e}") from e

    def rename_index(self, db_name: str, table_name: str, index_name: str, new_name: str):
        """重命名索引"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            if self.RENAME_INDEX_SQL is None:
                raise Exception(f"当前数据库类型 {self.DB_TYPE} 不支持 rename index")
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = True
                    if self.QUERY_INDEX_SQL is not None:
                        sql = self.QUERY_INDEX_SQL.format(db_name=db_name, table_name=table_name, index_name=index_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) == 0:
                            exist = False
                    if exist:
                        sql = self.RENAME_INDEX_SQL.format(db_name=db_name, table_name=table_name, index_name=index_name, new_name=new_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"rename_index: {db_name}.{table_name}.{index_name} 失败, 错误: {e}") from e

    def drop_index(self, db_name: str, table_name: str, index_name: str):
        """删除索引"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = True
                    if self.QUERY_INDEX_SQL is not None:
                        sql = self.QUERY_INDEX_SQL.format(db_name=db_name, table_name=table_name, index_name=index_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) == 0:
                            exist = False
                    if exist:
                        sql = self.DROP_INDEX_SQL.format(db_name=db_name, table_name=table_name, index_name=index_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"drop_index: {db_name}.{table_name}.{index_name} 失败, 错误: {e}") from e

    def add_constraint(self, db_name: str, table_name: str, constraint_name: str, constraint_property: str):
        """新增约束"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            if self.ADD_CONSTRAINT_SQL is None:
                raise Exception(f"当前数据库类型 {self.DB_TYPE} 不支持 add constraint")
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = False
                    if self.QUERY_CONSTRAINT_SQL is not None:
                        sql = self.QUERY_CONSTRAINT_SQL.format(db_name=db_name, table_name=table_name, constraint_name=constraint_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) > 0:
                            exist = True
                    if not exist:
                        sql = self.ADD_CONSTRAINT_SQL.format(
                            db_name=db_name, table_name=table_name,
                            constraint_name=constraint_name, constraint_property=constraint_property
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"add_constraint: {db_name}.{table_name}.{constraint_name} 失败, 错误: {e}") from e

    def rename_constraint(self, db_name: str, table_name: str, constraint_name: str, new_name: str):
        """重命名约束"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            if self.RENAME_CONSTRAINT_SQL is None:
                raise Exception(f"当前数据库类型 {self.DB_TYPE} 不支持 rename constraint")
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = True
                    if self.QUERY_CONSTRAINT_SQL is not None:
                        sql = self.QUERY_CONSTRAINT_SQL.format(db_name=db_name, table_name=table_name, constraint_name=constraint_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) == 0:
                            exist = False
                    if exist:
                        sql = self.RENAME_CONSTRAINT_SQL.format(
                            db_name=db_name, table_name=table_name,
                            constraint_name=constraint_name, new_name=new_name
                        )
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"rename_constraint: {db_name}.{table_name}.{constraint_name} 失败, 错误: {e}") from e

    def drop_constraint(self, db_name: str, table_name: str, constraint_name: str):
        """删除约束"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            if self.DROP_CONSTRAINT_SQL is None:
                raise Exception(f"当前数据库类型 {self.DB_TYPE} 不支持 drop constraint")
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    exist = True
                    if self.QUERY_CONSTRAINT_SQL is not None:
                        sql = self.QUERY_CONSTRAINT_SQL.format(db_name=db_name, table_name=table_name, constraint_name=constraint_name)
                        cursor.execute(sql)
                        rowlist = cursor.fetchall()
                        if len(rowlist) == 0:
                            exist = False
                    if exist:
                        sql = self.DROP_CONSTRAINT_SQL.format(db_name=db_name, table_name=table_name, constraint_name=constraint_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"drop_constraint: {db_name}.{table_name}.{constraint_name} 失败, 错误: {e}") from e

    def rename_table(self, db_name: str, table_name: str, new_name: str):
        """重命名表"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_TABLE_SQL.format(db_name=db_name, table_name=table_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) > 0:
                        sql = self.RENAME_TABLE_SQL.format(db_name=db_name, table_name=table_name, new_name=new_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"rename_table: {db_name}.{table_name} 失败, 错误: {e}") from e

    def drop_table(self, db_name: str, table_name: str):
        """删除表"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.SET_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    sql = self.QUERY_TABLE_SQL.format(db_name=db_name, table_name=table_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    if len(rowlist) > 0:
                        sql = self.DROP_TABLE_SQL.format(db_name=db_name, table_name=table_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"drop_table: {db_name}.{table_name} 失败, 错误: {e}") from e

    def drop_db(self, db_name: str):
        """删除数据库"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.DROP_DATABASE_SQL.format(db_name=db_name)
                    cursor.execute(sql)
        except Exception as e:
            raise Exception(f"drop_db: {db_name} 失败, 错误: {e}") from e

    def reset_schema(self, db_names: list):
        """重置数据库 schema"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(self.QUERY_DATABASES_SQL)
                    rowlist = cursor.fetchall()
                    current_dbs = [element[0] for element in rowlist]

                    for db_name in db_names:
                        if db_name in current_dbs:
                            sql = self.DROP_DATABASE_SQL.format(db_name=db_name)
                            cursor.execute(sql)
                        sql = self.CREATE_DATABASE_SQL.format(db_name=db_name)
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"reset_schema: {db_names} 失败, 错误: {e}") from e

    def _check_exists(self, cursor, query: str) -> bool:
        """执行查询并返回是否有结果"""
        cursor.execute(query)
        return len(cursor.fetchall()) > 0

    def _parse_object_name(self, qualified_name: str) -> str:
        """从可能带数据库/schema前缀的名称中提取对象名（去除引号和空白）"""
        if "." in qualified_name:
            parts = qualified_name.split(".")
            return self.get_real_name(parts[-1])
        return self.get_real_name(qualified_name)

    def run_sql(self, sql_list: list):
        """执行 SQL 语句列表（幂等）：CREATE/DROP 语句先查对象是否存在再决定是否执行"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    current_db = None
                    # 获取切库语句的首个关键字（USE / SET）用于跟踪 current_db
                    set_db_keyword = next_token(self.SET_DATABASE_SQL)[0].upper()
                    for sql in sql_list:
                        token, remaining = next_token(sql)
                        token = token.upper()

                        if token == set_db_keyword:
                            db = self.parse_sql_use_db(sql)
                            current_db = db.DBName
                            cursor.execute(sql)
                        elif token == "CREATE":
                            self._run_sql_create(cursor, current_db, sql, remaining)
                        elif token == "DROP":
                            self._run_sql_drop(cursor, current_db, sql, remaining)
                        elif token == "ALTER":
                            self._run_sql_alter(cursor, current_db, sql, remaining)
                        elif token == "RENAME":
                            self._run_sql_rename(cursor, current_db, sql, remaining)
                        else:
                            # INSERT, UPDATE, DELETE 等直接执行
                            cursor.execute(sql)
        except Exception as e:
            raise Exception(f"run_sql 失败, DB_TYPE: {self.DB_TYPE}, 错误: {e}") from e

    def _run_sql_create(self, cursor, current_db, sql, remaining):
        """处理 CREATE 语句的幂等执行"""
        token2, remaining2 = next_token(remaining)
        token2 = token2.upper()

        if token2 == "TABLE":
            # CREATE TABLE [IF NOT EXISTS] <name>
            token3, remaining3 = next_token(remaining2)
            if token3.upper() == "IF":
                _, remaining3 = next_tokens(remaining3, 2)  # skip NOT EXISTS
                token3, _ = next_token(remaining3)
            idx = token3.find("(")
            name_raw = token3[:idx] if idx != -1 else token3
            name = self.get_real_name(name_raw)
            check_sql = self.QUERY_TABLE_SQL.format(db_name=current_db, table_name=name)
            if self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] table {name} 已存在, 跳过: {sql}")
            else:
                cursor.execute(sql)

        elif token2 == "VIEW":
            # CREATE VIEW [IF NOT EXISTS] <name>
            token3, remaining3 = next_token(remaining2)
            if token3.upper() == "IF":
                _, remaining3 = next_tokens(remaining3, 2)  # skip NOT EXISTS
                token3, _ = next_token(remaining3)
            idx = token3.find("(")
            name_raw = token3[:idx] if idx != -1 else token3
            name = self.get_real_name(name_raw)
            check_sql = self.QUERY_VIEW_SQL.format(db_name=current_db, view_name=name)
            if self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] view {name} 已存在, 跳过: {sql}")
            else:
                cursor.execute(sql)

        elif token2 == "OR":
            # CREATE OR REPLACE VIEW — 天然幂等，直接执行
            cursor.execute(sql)

        elif token2 == "INDEX":
            # CREATE INDEX [IF NOT EXISTS] <name> ON <tbl>(...)
            self._run_sql_create_index(cursor, current_db, sql, remaining2)

        elif token2 == "UNIQUE":
            # CREATE UNIQUE INDEX [IF NOT EXISTS] <name> ON <tbl>(...)
            _, remaining3 = next_token(remaining2)  # skip INDEX
            self._run_sql_create_index(cursor, current_db, sql, remaining3)

        else:
            cursor.execute(sql)

    def _run_sql_drop(self, cursor, current_db, sql, remaining):
        """处理 DROP 语句的幂等执行"""
        token2, remaining2 = next_token(remaining)
        token2 = token2.upper()

        if token2 == "TABLE":
            # DROP TABLE [IF EXISTS] <name>
            token3, remaining3 = next_token(remaining2)
            if token3.upper() == "IF":
                _, remaining3 = next_token(remaining3)  # skip EXISTS
                token3, _ = next_token(remaining3)
            name = self.get_real_name(token3)
            check_sql = self.QUERY_TABLE_SQL.format(db_name=current_db, table_name=name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] 跳过: {sql}")
            else:
                cursor.execute(sql)

        elif token2 == "VIEW":
            # DROP VIEW [IF EXISTS] <name>
            token3, remaining3 = next_token(remaining2)
            if token3.upper() == "IF":
                _, remaining3 = next_token(remaining3)  # skip EXISTS
                token3, _ = next_token(remaining3)
            name = self.get_real_name(token3)
            check_sql = self.QUERY_VIEW_SQL.format(db_name=current_db, view_name=name)
            if not self._check_exists(cursor, check_sql):
                if self.logger:
                    self.logger.info(f"[run_sql] view {name} 不存在, 跳过: {sql}")
            else:
                cursor.execute(sql)

        elif token2 == "INDEX":
            self._run_sql_drop_index(cursor, current_db, sql, remaining2)

        else:
            cursor.execute(sql)

    def _run_sql_create_index(self, cursor, current_db, sql, remaining):
        """处理 CREATE [UNIQUE] INDEX 的幂等执行"""
        if self.QUERY_INDEX_SQL is None:
            cursor.execute(sql)
            return
        # [IF NOT EXISTS] <idx_name> ON <tbl>(...)
        token, remaining2 = next_token(remaining)
        if token.upper() == "IF":
            _, remaining2 = next_tokens(remaining2, 2)  # skip NOT EXISTS
            token, remaining2 = next_token(remaining2)
        idx_name = self.get_real_name(token)
        _, remaining2 = next_token(remaining2)  # skip ON
        tbl_token, _ = next_token(remaining2)
        idx = tbl_token.find("(")
        tbl_raw = tbl_token[:idx] if idx != -1 else tbl_token
        tbl_name = self._parse_object_name(tbl_raw)
        check_sql = self.QUERY_INDEX_SQL.format(db_name=current_db, table_name=tbl_name, index_name=idx_name)
        if self._check_exists(cursor, check_sql):
            if self.logger:
                self.logger.info(f"[run_sql] index {idx_name} 已存在, 跳过: {sql}")
        else:
            cursor.execute(sql)

    def _run_sql_drop_index(self, cursor, current_db, sql, remaining):
        """处理 DROP INDEX 的幂等执行，默认直接执行（依赖 SQL 自带的 IF EXISTS），子类可 override"""
        cursor.execute(sql)

    def _run_sql_alter(self, cursor, current_db, sql, remaining):
        """处理 ALTER 语句的幂等执行，默认直接执行，子类可 override"""
        cursor.execute(sql)

    def _run_sql_rename(self, cursor, current_db, sql, remaining):
        """处理 RENAME 语句的幂等执行，默认直接执行，子类可 override"""
        cursor.execute(sql)

    def list_tables_by_db(self, db_name: str) -> list:
        """获取数据库所有表名"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.QUERY_TABLES_SQL.format(db_name=db_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    return [row[0] for row in rowlist]
        except Exception as e:
            raise Exception(f"list_tables_by_db: {db_name} 失败, 错误: {e}")

    def get_table_columns(self, db_name: str, table_name: str) -> dict:
        """获取表结构"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    sql = self.QUERY_COLUMNS_SQL.format(db_name=db_name, table_name=table_name)
                    cursor.execute(sql)
                    columns = [desc[0] for desc in cursor.description]
                    rowlist = cursor.fetchall()
                    schema = {}
                    for row in rowlist:
                        row_dict = dict(zip(columns, row))
                        col_name = row_dict[self.COLUMN_NAME_FIELD].upper()
                        schema[col_name] = row_dict
                    return schema
        except Exception as e:
            raise Exception(f"get_table_columns: {db_name}.{table_name} 失败, 错误: {e}")
