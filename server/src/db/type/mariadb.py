#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.type.mysql import MysqlRDS
from dataModelManagement.src.utils.log_util import Logger as Log


class MariaDBRDS(MysqlRDS):
  """
  MariaDB RDS实现类
  """

  def __init__(self, rds_info: RDSInfo, logger: Log):
    # 调用基类的初始化方法
    super().__init__(rds_info, logger)

    self.SET_DATABASE_SQL = "USE {db_name}"
    self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
    self.CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS {db_name} CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    self.DROP_DATABASE_SQL = "DROP DATABASE IF EXISTS {db_name}"

    self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}'"
    self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
    self.RENAME_TABLE_SQL = "RENAME TABLE IF EXISTS {db_name}.{table_name} TO {new_name}"
    self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.{table_name}"

    self.COLUMN_NAME_FIELD = "COLUMN_NAME"
    self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
    self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
    self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
    self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} MODIFY COLUMN IF EXISTS {column_name} {column_property} COMMENT '{column_comment}'"
    self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} RENAME COLUMN IF EXISTS {column_name} TO {new_name}"
    self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} DROP COLUMN IF EXISTS {column_name}"

    self.QUERY_INDEX_SQL = "SHOW INDEX FROM {db_name}.{table_name} WHERE Key_name = '{index_name}'"
    self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.{table_name} ({index_property}) COMMENT '{index_comment}'"
    self.RENAME_INDEX_SQL = "ALTER TABLE {db_name}.{table_name} RENAME INDEX {index_name} TO {new_name}"
    self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {index_name} ON {db_name}.{table_name}"

    self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
      WHERE CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""
    self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_property}"
    self.RENAME_CONSTRAINT_SQL = None
    self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraint_name}"

  def create_user(self, user_name: str, password: str):
    # 1. 单个账户不创建用户
    if self.admin_user == user_name:
      return
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute(f"CREATE USER '{user_name}'@'%' IDENTIFIED BY '{password}';")
    except Exception as e:
      raise Exception(f"create user failed, error:{e}")
