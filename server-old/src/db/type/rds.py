import base64
from abc import ABC, abstractmethod
from typing import List

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.connection import DatabaseConnection
from dataModelManagement.src.utils.log_util import Logger as Log


class RDS(ABC):
  """
  抽象基类，定义创建用户的方法。
  """

  def __init__(self, rds_info: RDSInfo, logger: Log):
    self.logger = logger
    self.rds_info = rds_info
    self.DB_TYPE = self.rds_info.type
    self.database_connection = DatabaseConnection(self.rds_info)
    self._root_conn_db = None
    self._conn_db = None
    self.admin_user = ""
    self.admin_password = ""

  def _init_root_conn(self):
    """初始化数据库连接(root)"""
    if self._root_conn_db is None:
      try:
        self._root_conn_db = self.database_connection.get_root_conn()
      except Exception as e:
        raise Exception(f"get connection fail, error:{e}")
      admin_key = base64.b64decode(self.rds_info.admin_key).decode("utf-8")
      db_information = admin_key.split(":")
      self.admin_user = db_information[0]
      self.admin_password = db_information[1]
      self.RwPrivileges = "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, CREATE TEMPORARY TABLES, LOCK TABLES, EXECUTE, CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER"

  def _init_conn(self):
    """初始化数据库连接"""
    if self._conn_db is None:
      try:
        self._conn_db = self.database_connection.get_conn()
      except Exception as e:
        raise Exception(f"get connection fail, error:{e}")

  @abstractmethod
  def create_user(self, user_name: str, password: str):
    pass

  @abstractmethod
  def set_user_privilege(self, user_name: str, databases: List[str]):
    pass

  @abstractmethod
  def create_db(self, db_name: str):
    pass

  @abstractmethod
  def add_user_privilege(self, user_name: str, databases: List[str]):
    pass

  @abstractmethod
  def select_all_databases_root(self):
    pass

  @abstractmethod
  def select_all_databases_user(self):
    pass

  @abstractmethod
  def select_user_form_database(self, user_name: str):
    pass

  @abstractmethod
  def init_db_config(self):
    pass

  @abstractmethod
  def init_db_after(self):
    pass

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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
        sql = self.SET_DATABASE_SQL.format(db_name=db_name)
        cursor.execute(sql)
        sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
        cursor.execute(sql)
        rowlist = cursor.fetchall()
        if len(rowlist) == 0:
          sql = self.ADD_COLUMN_SQL.format(
            db_name=db_name,
            table_name=table_name,
            column_name=column_name,
            column_property=column_property,
            column_comment=column_comment,
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
        sql = self.SET_DATABASE_SQL.format(db_name=db_name)
        cursor.execute(sql)
        sql = self.QUERY_COLUMN_SQL.format(db_name=db_name, table_name=table_name, column_name=column_name)
        cursor.execute(sql)
        rowlist = cursor.fetchall()
        if len(rowlist) > 0:
          sql = self.MODIFY_COLUMN_SQL.format(
            db_name=db_name,
            table_name=table_name,
            column_name=column_name,
            column_property=column_property,
            column_comment=column_comment,
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      if self.RENAME_INDEX_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support rename index")

      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      if self.ADD_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support add constraint")

      self._init_conn()
      with self._conn_db.cursor() as cursor:
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
            db_name=db_name,
            table_name=table_name,
            constraint_name=constraint_name,
            constraint_property=constraint_property,
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

    try:
      if self.RENAME_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support rename constraint")

      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      if self.DROP_CONSTRAINT_SQL is None:
        raise Exception(f"current db_type {self.DB_TYPE} does not support drop constraint")

      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
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

    try:
      self._init_conn()
      with self._conn_db.cursor() as cursor:
        sql = self.DROP_DATABASE_SQL.format(db_name=db_name)
        print(sql)
        cursor.execute(sql)
    except Exception as e:
      print(f"drop_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}")
      raise Exception(f"drop_db: {db_name}失败, DB_TYPE: {self.DB_TYPE}, 错误信息: {e}") from e
