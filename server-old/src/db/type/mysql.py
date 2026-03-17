#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.type.rds import RDS
from dataModelManagement.src.utils.log_util import Logger as Log


class MysqlRDS(RDS):
  """
  MySQL RDS实现类
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
    self.RENAME_TABLE_SQL = "RENAME TABLE {db_name}.{table_name} TO {new_name}"
    self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.{table_name}"

    self.COLUMN_NAME_FIELD = "COLUMN_NAME"
    self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
    self.QUERY_COLUMN_SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
    self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} ADD COLUMN {column_name} {column_property} COMMENT '{column_comment}'"
    self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} MODIFY COLUMN {column_name} {column_property} COMMENT '{column_comment}'"
    self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} CHANGE COLUMN {column_name} {new_name} {column_property} COMMENT '{column_comment}'"
    self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.{table_name} DROP COLUMN {column_name}"

    self.QUERY_INDEX_SQL = "SHOW INDEX FROM {db_name}.{table_name} WHERE Key_name = '{index_name}'"
    self.ADD_INDEX_SQL = "CREATE {index_type} {index_name} ON {db_name}.{table_name} ({index_property}) COMMENT '{index_comment}'"
    self.RENAME_INDEX_SQL = "ALTER TABLE {db_name}.{table_name} RENAME INDEX {index_name} TO {new_name}"
    self.DROP_INDEX_SQL = "DROP INDEX {index_name} ON {db_name}.{table_name}"

    self.QUERY_CONSTRAINT_SQL = """SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
      WHERE CONSTRAINT_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"""
    self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.{table_name} ADD CONSTRAINT {constraint_name} {constraint_property}"
    self.RENAME_CONSTRAINT_SQL = None
    self.DROP_CONSTRAINT_SQL = None

  def init_db_after(self):
    pass

  def init_db_config(self):
    """初始化数据库配置"""
    pass

  def add_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    # 2. 删除权限
    # 3. 重新添加权限
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        for db in databases:
          cursor.execute(f"GRANT {self.RwPrivileges} ON {db}.* TO '{user_name}'@'%';")
          cursor.execute("FLUSH PRIVILEGES;")
    except Exception as e:
      raise Exception(f"add user privilege failed, error:{e}")

  def set_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    # 2. 删除权限
    # 3. 重新添加权限
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        for db in databases:
          cursor.execute(f"GRANT {self.RwPrivileges} ON {db}.* TO '{user_name}'@'%'; ")
          # print(f"GRANT {self.RwPrivileges} ON {db}.* TO '{user_name}'@'%'; ")
          cursor.execute("FLUSH PRIVILEGES;")
    except Exception as e:
      raise Exception(f"set user privilege failed, error:{e}")

  def create_db(self, db_name: str):
    """创建数据库"""
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        sql = self.CREATE_DATABASE_SQL.format(db_name=db_name)
        cursor.execute(sql)
    except Exception as e:
      raise Exception(f"create db failed, error:{e}")

  def select_all_databases_root(self):
    """查询所有的数据库"""
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        sql = "show databases"
        cursor.execute(sql)
        results = cursor.fetchall()
        db_list = [item["Database"] for item in results]
        return db_list
    except Exception as e:
      raise Exception(f"select all databases failed, error:{e}")

  def select_all_databases_user(self):
    """查询所有的数据库"""
    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
        sql = "show databases"
        cursor.execute(sql)
        results = cursor.fetchall()
        db_list = [item["Database"] for item in results]
        return db_list
    except Exception as e:
      raise Exception(f"select all databases failed, error:{e}")

  def select_user_form_database(self, user_name):
    """查询指定用户"""
    # 1. 单个账户不查询用户是否存在
    if self.admin_user == user_name:
      return
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        sql = """SELECT User FROM mysql.user WHERE User = %s """
        cursor.execute(sql, user_name)
        result = cursor.fetchone()
        return result
    except Exception as e:
      raise Exception(f"select_user failed, error:{e}")

  def create_user(self, user_name: str, password: str):
    # 1. 单个账户不创建用户
    if self.admin_user == user_name:
      return
    try:
      self._init_root_conn()
      with self._root_conn_db.cursor() as cursor:
        cursor.execute(f"CREATE USER '{user_name}'@'%' IDENTIFIED WITH mysql_native_password BY '{password}';")
    except Exception as e:
      raise Exception(f"create user failed, error:{e}")
