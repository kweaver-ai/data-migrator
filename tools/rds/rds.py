#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from abc import ABC, abstractmethod
import os

import rdsdriver

from utils.check_config import CheckConfig


class CheckRDS(ABC):
  """RDS检查接口类"""

  def __init__(self, check_config: CheckConfig):
    """初始化RDS检查器"""
    self.check_config = check_config

  @abstractmethod
  def check_init(self, sql_list: list[str]):
    """
    检查init sql是否合法

    Args:
      sql_list: 要检查的sql语句列表
    """
    pass

  @abstractmethod
  def check_update(self, sql_list: list[str]):
    """
    检查update sql是否合法

    Args:
      sql_list: 要检查的sql语句列表
    """
    pass

  @abstractmethod
  def get_column_type(self, column: dict) -> tuple[str, str]:
    """
    获取字段的类型和分类

    Args:
      column: 字段信息
    """
    pass

  @abstractmethod
  def parse_sql_column_define(self, column_name: str, remaining_sql: str):
    """
    解析字段定义sql

    Args:
      column_name: 字段名
      remaining_sql: 剩余的sql语句
    """
    pass

  def reset_schema(self, db_names: list[str]):
    """
    重置数据库schema

    Args:
      db_names: 要重置的数据库名列表
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
        with conn.cursor() as cursor:
          # 查询所有数据库
          cursor.execute(self.QUERY_DATABASES_SQL)
          rowlist = cursor.fetchall()
          current_dbs = [element[0] for element in rowlist]

          for db_name in db_names:
            # 1.删库
            if db_name in current_dbs:
              sql = self.DROP_DATABASE_SQL.format(db_name=db_name)
              print(sql)
              cursor.execute(sql)

            # 2.建库
            sql = self.CREATE_DATABASE_SQL.format(db_name=db_name)
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"reset_schema: {db_names}失败, DB_INFO: {self.DB_CONFIG_ROOT}, 错误信息: {e}")
      raise Exception(f"reset_schema: {db_names}失败, DB_INFO: {self.DB_CONFIG_ROOT}, 错误信息: {e}") from e

  def run_sql(self, sql_list: list[str]):
    """
    运行sql语句

    Args:
      sql_list: 要运行的sql语句列表
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
        with conn.cursor() as cursor:
          for sql in sql_list:
            print(f"\nexecute sql: {sql}")
            cursor.execute(sql)
    except Exception as e:
      print(f"run_sql: {sql}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"run_sql: {sql}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def add_column(self, db_name: str, table_name: str, column_name: str, column_property: str, column_comment: str):
    """
    新增字段

    Args:
      db_name: 数据库名
      table_name: 表名
      column_name: 字段名
      column_property: 字段属性
      column_comment: 字段注释
    """
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
              db_name=db_name, table_name=table_name, column_name=column_name, column_property=column_property, column_comment=column_comment
            )
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"add_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"add_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def modify_column(self, db_name: str, table_name: str, column_name: str, column_property: str, column_comment: str):
    """
    修改字段

    Args:
      db_name: 数据库名
      table_name: 表名
      column_name: 字段名
      column_property: 新的字段属性
      column_comment: 新的字段注释
    """
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
              db_name=db_name, table_name=table_name, column_name=column_name, column_property=column_property, column_comment=column_comment
            )
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"modify_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"modify_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def rename_column(self, db_name: str, table_name: str, column_name: str, new_name: str, column_property: str, column_comment: str):
    """
    重命名列

    Args:
      db_name: 数据库名
      table_name: 表名
      column_name: 列名
      new_name: 新的列名
      column_property: 列属性
      column_comment: 列注释
    """
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
              db_name=db_name,
              table_name=table_name,
              column_name=column_name,
              new_name=new_name,
              column_property=column_property,
              column_comment=column_comment,
            )
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"rename_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"rename_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def drop_column(self, db_name: str, table_name: str, column_name: str):
    """
    删除列

    Args:
      db_name: 数据库名
      table_name: 表名
      column_name: 列名
    """
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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"drop_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_column: {db_name}.{table_name}.{column_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def add_index(self, db_name: str, table_name: str, index_type: str, index_name: str, index_property: str, index_comment: str):
    """
    新增索引

    Args:
      db_name: 数据库名
      table_name: 表名
      index_type: 索引类型
      index_name: 索引名
      index_property: 索引属性
      index_comment: 索引注释
    """
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
              db_name=db_name,
              table_name=table_name,
              index_type=index_type,
              index_name=index_name,
              index_property=index_property,
              index_comment=index_comment,
            )
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"add_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"add_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def rename_index(self, db_name: str, table_name: str, index_name: str, new_name: str):
    """
    重命名索引

    Args:
      db_name: 数据库名
      table_name: 表名
      index_name: 索引名
      new_name: 新的索引名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      if self.RENAME_INDEX_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support rename index")

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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"rename_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"rename_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def drop_index(self, db_name: str, table_name: str, index_name: str):
    """
    删除索引

    Args:
      db_name: 数据库名
      table_name: 表名
      index_name: 索引名
    """
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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"drop_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_index: {db_name}.{table_name}.{index_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def add_constraint(self, db_name: str, table_name: str, constraint_name: str, constraint_property: str):
    """
    新增约束

    Args:
      db_name: 数据库名
      table_name: 表名
      constraint_name: 约束名
      constraint_property: 约束属性
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      if self.ADD_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support add constraint")

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
              db_name=db_name, table_name=table_name, constraint_name=constraint_name, constraint_property=constraint_property
            )
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"add_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"add_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def rename_constraint(self, db_name: str, table_name: str, constraint_name: str, new_name: str):
    """
    重命名约束

    Args:
      db_name: 数据库名
      table_name: 表名
      constraint_name: 约束名
      new_name: 新的约束名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      if self.RENAME_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support rename constraint")

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
            sql = self.RENAME_CONSTRAINT_SQL.format(db_name=db_name, table_name=table_name, constraint_name=constraint_name, new_name=new_name)
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"rename_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"rename_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def drop_constraint(self, db_name: str, table_name: str, constraint_name: str):
    """
    删除约束

    Args:
      db_name: 数据库名
      table_name: 表名
      constraint_name: 约束名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      if self.DROP_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support drop constraint")

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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"drop_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_constraint: {db_name}.{table_name}.{constraint_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def rename_table(self, db_name: str, table_name: str, new_name: str):
    """
    重命名表

    Args:
      db_name: 数据库名
      table_name: 表名
      new_name: 新表名
    """
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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"rename_table: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"rename_table: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def drop_table(self, db_name: str, table_name: str):
    """
    删除表

    Args:
      db_name: 数据库名
      table_name: 表名
    """
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
            print(sql)
            cursor.execute(sql)
    except Exception as e:
      print(f"drop_table: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_table: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def drop_db(self, db_name: str):
    """
    删除数据库

    Args:
      db_name: 数据库名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
        with conn.cursor() as cursor:
          sql = self.DROP_DATABASE_SQL.format(db_name=db_name)
          print(sql)
          cursor.execute(sql)
    except Exception as e:
      print(f"drop_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e

  def list_tables_by_db(self, db_name: str):
    """
    获取数据库所有表名

    Args:
      db_name: 数据库名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      with rdsdriver.connect(**self.DB_CONFIG_ROOT) as conn:
        with conn.cursor() as cursor:
          sql = self.QUERY_TABLES_SQL.format(db_name=db_name)
          cursor.execute(sql)
          rowlist = cursor.fetchall()
          table_list = []
          for row in rowlist:
            table_list.append(row[0])
          return table_list
    except Exception as e:
      print(f"list_tables_by_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"list_tables_by_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")

  def get_table_columns(self, db_name: str, table_name: str):
    """
    获取表结构

    Args:
      db_name: 数据库名
      table_name: 表名
    """
    os.environ["DB_TYPE"] = self.DB_TYPE

    try:
      with rdsdriver.connect(**self.DB_CONFIG_ROOT, cursorclass=rdsdriver.DictCursor) as conn:
        with conn.cursor() as cursor:
          sql = self.QUERY_COLUMNS_SQL.format(db_name=db_name, table_name=table_name)
          cursor.execute(sql)
          rowlist = cursor.fetchall()
          schema_list = {}
          for row in rowlist:
            column_name = row[self.COLUMN_NAME_FIELD].upper()
            schema_list[column_name] = row
          return schema_list
    except Exception as e:
      print(f"get_table_columns: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"get_table_columns: {db_name}.{table_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
