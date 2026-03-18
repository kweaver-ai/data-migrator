#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDS 校验抽象基类 - 复用 tools/rds/rds.py"""
from abc import ABC, abstractmethod
import os
from logging import Logger
from pathlib import Path

import yaml
import rdsdriver

from server.check.check_config import CheckConfig

# 默认配置文件路径（可通过环境变量 CHECK_RDS_CONFIG 覆盖）
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "check_rds_config.yaml"


def load_rds_config() -> dict:
    """加载 RDS 校验连接配置"""
    config_path = os.environ.get("CHECK_RDS_CONFIG", str(_DEFAULT_CONFIG_PATH))
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


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

    def run_sql(self, sql_list: list):
        """执行 SQL 语句列表"""
        os.environ["DB_TYPE"] = self.DB_TYPE
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
                with conn.cursor() as cursor:
                    for sql in sql_list:
                        cursor.execute(sql)
        except Exception as e:
            raise Exception(f"run_sql 失败, DB_TYPE: {self.DB_TYPE}, 错误: {e}") from e

    def list_tables_by_db(self, db_name: str) -> list:
        """获取数据库所有表名"""
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
        try:
            with rdsdriver.connect(**self.DB_CONFIG_ROOT, cursorclass=rdsdriver.DictCursor) as conn:
                with conn.cursor() as cursor:
                    sql = self.QUERY_COLUMNS_SQL.format(db_name=db_name, table_name=table_name)
                    cursor.execute(sql)
                    rowlist = cursor.fetchall()
                    schema = {}
                    for row in rowlist:
                        col_name = row[self.COLUMN_NAME_FIELD].upper()
                        schema[col_name] = row
                    return schema
        except Exception as e:
            raise Exception(f"get_table_columns: {db_name}.{table_name} 失败, 错误: {e}")
