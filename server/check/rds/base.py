#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDS 校验抽象基类 - 复用 tools/rds/rds.py"""
from abc import ABC, abstractmethod
import os
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

    def __init__(self, check_config: CheckConfig):
        self.check_config = check_config

    @abstractmethod
    def check_init(self, sql_list: list):
        pass

    @abstractmethod
    def check_update(self, sql_list: list):
        pass

    @abstractmethod
    def get_column_type(self, column: dict) -> tuple:
        pass

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
