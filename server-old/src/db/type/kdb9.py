#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List

from dataModelManagement.src.utils.log_util import Logger as Log
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.type.rds import RDS


class KingBaseRDS(RDS):
  def __init__(self, rds_info: RDSInfo, logger: Log):
    # 调用基类的初始化方法
    super().__init__(rds_info, logger)

    self.SET_DATABASE_SQL = "SET SEARCH_PATH TO {db_name}"
    self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name='proton'"
    self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
    self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"

    self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}'"
    self.QUERY_TABLE_SQL = (
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
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

  def init_db_after(self):
    pass

  def init_db_config(self):
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute("ALTER SYSTEM SET sql_mode='ANSI_QUOTES';")
        # cursor.execute(f"ALTER SYSTEM SET standard_conforming_strings = off;")
        cursor.execute(self.SET_SCHEMA_SQL.format("deploy"))
        cursor.execute(self.CREATE_FUNCTION_HEX)
        cursor.execute(self.CREATE_FUNCTION_JSON_EXTRACT)
        cursor.execute(self.ALTER_FUNCTION_JSON_EXTRACT)
        # cursor.execute(self.CREATE_EXTENSION_MYSQL_JSON)
        # cursor.execute(self.ALTER_SYSTEM_SET_STANDARD_CONFORMING_STRINGS)
        cursor.execute("SELECT sys_reload_conf();")
    except Exception as e:
      raise Exception(f"init kdb config fail,error:{e}")

  def add_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The KDB database currently does not support automated user privilege management.")
      return

  def set_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The KDB database currently does not support automated user privilege management.")
      return

  def create_db(self, db_name: str):
    """创建数据库"""
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        # 建模式
        query_sql = self.CREATE_SCHEMA_SQL.format(db_name)
        cursor.execute(query_sql)
        cursor.execute(self.SET_SCHEMA_SQL.format(db_name))
        cursor.execute(self.CREATE_FUNCTION_HEX)
    except Exception as e:
      raise Exception(f"create db fail,error:{e}") from e

  def select_all_databases_root(self):
    """查询所有的数据库"""
    sql = "select schema_name from INFORMATION_SCHEMA.SCHEMATA where catalog_name='proton';"
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()
    except Exception as e:
      raise Exception(f"select all schema fail,error:{e}") from e
    db_list = [item["schema_name"] for item in results]
    return db_list

  def select_all_databases_user(self):
    """查询所有的数据库"""
    sql = "select schema_name from INFORMATION_SCHEMA.SCHEMATA where catalog_name='proton';"
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute(sql)
        results = cursor.fetchall()
        db_list = [item["schema_name"] for item in results]
        return db_list
    except Exception as e:
      raise Exception(f"select all schema fail,error:{e}") from e

  def select_user_form_database(self, user_name: str):
    """查询指定用户"""
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute(f"""select * from sys_user where usename='{user_name}';""")
        result = cursor.fetchone()
        return result
    except Exception as e:
      raise Exception(f"select_user fail,error:{e}")

  def create_user(self, user_name: str, password: str):
    # 1. 单个账户不创建用户
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The KDB database currently does not support automated user privilege management.")
      return

  CREATE_SCHEMA_SQL = """
  CREATE SCHEMA {};
  """

  CREATE_FUNCTION_HEX = """
  CREATE OR REPLACE FUNCTION hex(input_text TEXT)
    RETURNS BYTEA AS $$
    BEGIN
      RETURN input_text::BYTEA;
    END;
  $$ LANGUAGE plpgsql;
  """

  SET_SCHEMA_SQL = """
  SET SEARCH_PATH TO {};
  """

  CREATE_FUNCTION_JSON_EXTRACT = """
  CREATE OR REPLACE FUNCTION sys.json_extract(text, VARIADIC jsonpath[])
  RETURNS json
  LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
  AS '$libdir/mysql_json', 'JsonExtract';
  """

  ALTER_FUNCTION_JSON_EXTRACT = """
  ALTER FUNCTION sys.json_extract(text, VARIADIC jsonpath[]) IMMUTABLE;
  """

  ALTER_SYSTEM_SET_STANDARD_CONFORMING_STRINGS = """
  ALTER SYSTEM SET standard_conforming_strings = off;
  """
