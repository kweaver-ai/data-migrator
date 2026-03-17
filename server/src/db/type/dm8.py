#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import List

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.type.rds import RDS
from dataModelManagement.src.utils.log_util import Logger as Log


class DM8RDS(RDS):
  def __init__(self, rds_info: RDSInfo, logger: Log):
    # 调用基类的初始化方法
    super().__init__(rds_info, logger)

    self.SET_DATABASE_SQL = "SET SCHEMA {db_name}"
    self.QUERY_DATABASES_SQL = "select OWNER from dba_objects where object_type='SCH'"
    self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
    self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"

    self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}'"
    self.QUERY_TABLE_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
    self.RENAME_TABLE_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME TO {new_name}"
    self.DROP_TABLE_SQL = "DROP TABLE IF EXISTS {db_name}.\"{table_name}\" CASCADE"

    self.COLUMN_NAME_FIELD = "COLUMN_NAME"
    self.QUERY_COLUMNS_SQL = "SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
    self.QUERY_COLUMN_SQL = (
      "SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND COLUMN_NAME='{column_name}'"
    )
    self.ADD_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" ADD COLUMN IF NOT EXISTS {column_name} {column_property}"
    self.MODIFY_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" MODIFY {column_name} {column_property}"
    self.RENAME_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME COLUMN {column_name} TO {new_name}"
    self.DROP_COLUMN_SQL = "ALTER TABLE {db_name}.\"{table_name}\" DROP COLUMN IF EXISTS {column_name}"

    self.QUERY_INDEX_SQL = "SELECT * FROM ALL_INDEXES WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND index_name='{index_name}'"
    self.ADD_INDEX_SQL = "CREATE {index_type} IF NOT EXISTS {index_name} ON {db_name}.\"{table_name}\" ({index_property})"
    self.RENAME_INDEX_SQL = "ALTER INDEX {db_name}.{index_name} RENAME TO {new_name}"
    self.DROP_INDEX_SQL = "DROP INDEX IF EXISTS {db_name}.{index_name}"

    self.QUERY_CONSTRAINT_SQL = (
      "SELECT * FROM ALL_CONSTRAINTS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}' AND CONSTRAINT_NAME='{constraint_name}'"
    )
    self.ADD_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" ADD CONSTRAINT {constraint_name} {constraint_property}"
    self.RENAME_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" RENAME CONSTRAINT {constraint_name} TO {new_name}"
    self.DROP_CONSTRAINT_SQL = "ALTER TABLE {db_name}.\"{table_name}\" DROP CONSTRAINT {constraint_name} CASCADE"

  def init_db_after(self):
    pass

  def init_db_config(self):
    """初始化数据库配置"""
    try:
      self._init_root_conn()
      cursor = self._root_conn_db.cursor()
      cursor.execute("SP_SET_PARA_VALUE(1,'GROUP_OPT_FLAG',1);")
      cursor.execute("SP_SET_PARA_VALUE(1,'ENABLE_BLOB_CMP_FLAG',1);")
      cursor.execute("SP_SET_PARA_VALUE(1,'PK_WITH_CLUSTER',0);")
      cursor.execute("alter system set 'COMPATIBLE_MODE'=4 spfile;")
      cursor.execute("alter system set 'MVCC_RETRY_TIMES'=15 spfile;")
      cursor.close()
    except Exception as e:
      raise Exception(f"init dm db config,error:{e}") from e

  def add_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The DM8 database currently does not support automated user privilege management.")
      return

  def set_user_privilege(self, user_name: str, databases: List[str]):
    # 1. 检查是否未为admin用户，admin用户不需要添加权限，直接建库
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The DM8 database currently does not support automated user privilege management.")
      return

  def create_db(self, db_name: str):
    """创建数据库"""
    try:
      self._init_root_conn()
      cursor = self._root_conn_db.cursor()
      # 建模式
      query_sql = self.CREATE_SCHEMA_SQL.format(db_name)
      cursor.execute(query_sql)
      # 切模式
      query_sql = self.SET_SCHEMA_SQL.format(db_name)
      cursor.execute(query_sql)
      cursor.execute(self.INIT_FUNCTION01)
      cursor.execute(self.INIT_FUNCTION02)
      cursor.execute(self.INIT_FUNCTION03)
      cursor.execute(self.INIT_FUNCTION04)
    except Exception as e:
      raise Exception(f"create db fail,error:{e}") from e

  def select_all_databases_root(self):
    """查询所有的数据库"""
    sql = "select OWNER from dba_objects where object_type='SCH';"
    try:
      self._init_root_conn()
      cursor = self._root_conn_db.cursor()
      cursor.execute(sql)
      results = cursor.fetchall()
      cursor.close()
    except Exception as e:
      raise Exception(f"select all databases fail,error:{e}") from e
    db_list = [item["OWNER"] for item in results]
    return db_list

  def select_all_databases_user(self):
    """查询所有的数据库"""
    sql = "select OWNER from dba_objects where object_type='SCH';"
    try:
      self._init_root_conn()
      cursor = self._root_conn_db.cursor()
      cursor.execute(sql)
      results = cursor.fetchall()
      cursor.close()
    except Exception as e:
      raise Exception(f"select all databases fail,error:{e}") from e
    db_list = [item["OWNER"] for item in results]
    return db_list

  def select_user_form_database(self, user_name: str):
    """查询指定用户"""
    try:
      self._init_root_conn()
      cursor = self._root_conn_db.cursor()
      cursor.execute(f"""SELECT username FROM dba_users WHERE username='{user_name}';""")
      result = cursor.fetchone()
      cursor.close()
    except Exception as e:
      raise Exception(f"select_user fail,error:{e}")
    return result

  def create_user(self, user_name: str, password: str):
    # 1. 单个账户不创建用户
    if self.admin_user == user_name:
      return
    else:
      self.logger.error("The DM8 database currently does not support automated user privilege management.")
      return

  CREATE_SCHEMA_SQL = """
  CREATE SCHEMA {};
  """
  SET_SCHEMA_SQL = """
  SET SCHEMA {};
  """
  INIT_FUNCTION01 = """
  CREATE OR REPLACE TYPE bit_or_agg_type AS OBJECT (
    bit_or_result NUMBER(38),
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT bit_or_agg_type)
      RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateIterate(self IN OUT bit_or_agg_type, value IN NUMBER)
      RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateTerminate(self IN bit_or_agg_type, returnValue OUT NUMBER, flags IN NUMBER)
      RETURN NUMBER,
    MEMBER FUNCTION ODCIAggregateMerge(self IN OUT bit_or_agg_type, ctx2 IN bit_or_agg_type)
      RETURN NUMBER
  );
  """
  INIT_FUNCTION02 = """
  CREATE OR REPLACE TYPE BODY bit_or_agg_type IS
    STATIC FUNCTION ODCIAggregateInitialize(sctx IN OUT bit_or_agg_type)
      RETURN NUMBER IS
    BEGIN
      sctx := bit_or_agg_type(0);
      RETURN ODCIConst.Success;
    END;
    MEMBER FUNCTION ODCIAggregateIterate(self IN OUT bit_or_agg_type, value IN NUMBER)
      RETURN NUMBER IS
    BEGIN
      self.bit_or_result := COALESCE(self.bit_or_result, 0) | value;
      RETURN ODCIConst.Success;
    END;
    MEMBER FUNCTION ODCIAggregateTerminate(self IN bit_or_agg_type, returnValue OUT NUMBER, flags IN NUMBER)
      RETURN NUMBER IS
    BEGIN
      returnValue := self.bit_or_result;
      RETURN ODCIConst.Success;
    END;
    MEMBER FUNCTION ODCIAggregateMerge(self IN OUT bit_or_agg_type, ctx2 IN bit_or_agg_type)
      RETURN NUMBER IS
    BEGIN
      self.bit_or_result := COALESCE(self.bit_or_result, 0) | COALESCE(ctx2.bit_or_result, 0);
      RETURN ODCIConst.Success;
    END;
  END;
  """

  INIT_FUNCTION03 = """
  CREATE OR REPLACE FUNCTION bit_or(input NUMBER)
    RETURN NUMBER PARALLEL_ENABLE AGGREGATE USING bit_or_agg_type;
  """

  INIT_FUNCTION04 = """
  CREATE OR REPLACE FUNCTION uuid
  RETURN varchar(36)
  AS
  BEGIN
    RETURN newid();
  END;
  """
