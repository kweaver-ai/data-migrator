#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from rds.rds import CheckRDS
from utils.check_config import CheckConfig
from utils.table_define import Database, Table, Index, PrimaryIndex, UniqueIndex, Column
from utils.util import next_token, next_tokens


class CheckMariaDB(CheckRDS):
  """MariaDB RDS检查实现类"""

  def __init__(self, check_config: CheckConfig, is_primary: bool = True):
    """初始化MariaDB检查器"""

    self.DB_TYPE = "MARIADB"

    if is_primary:
      self.DB_CONFIG_ROOT = {
        "host": "10.4.134.224",
        "user": "root",
        "password": "eisoo.com",
        "port": 3330,
        "charset": "utf8mb4",
        "autocommit": True,
        "DB_TYPE": self.DB_TYPE,
      }
    else:
      self.DB_CONFIG_ROOT = {
        "host": "10.4.134.224",
        "user": "root",
        "password": "eisoo.com",
        "port": 3331,
        "charset": "utf8mb4",
        "autocommit": True,
        "DB_TYPE": self.DB_TYPE,
      }

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

    super().__init__(check_config)

  def get_column_type(self, column: dict) -> tuple[str, str]:
    """
    获取字段的类型和分类

    Args:
      column: 字段信息
    """
    column_type = None
    data_type = column["DATA_TYPE"].upper()
    if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT", "BOOLEAN"):
      column_type = "IntegerType"
    elif data_type in ("DECIMAL", "NUMERIC"):
      column_type = "FixedPointType"
    elif data_type in ("FLOAT", "DOUBLE"):
      column_type = "FloatingPointType"
    elif data_type in ("BIT"):
      column_type = "BitValueType"
    elif data_type in (
      "CHAR",
      "VARCHAR",
      "BINARY",
      "VARBINARY",
      "TINYBLOB",
      "BLOB",
      "MEDIUMBLOB",
      "LONGBLOB",
      "TINYTEXT",
      "TEXT",
      "MEDIUMTEXT",
      "LONGTEXT",
    ):
      column_type = "StringType"
    elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
      column_type = "DateAndTimeType"
    else:
      column_type = "UNKNOWN"
    return data_type, column_type

  def check_init(self, sql_list: list[str]):
    if len(sql_list) == 0:
      return

    sql = sql_list[0]
    token, remaining_sql = next_token(sql)
    token = token.upper()
    if token != "USE":
      raise Exception(f"init文件中第一条语句必须为 'USE': {sql}")

    db = self.parse_sql_use_db(sql)
    if db is None:
      raise Exception(f"USE Database语法错误: {sql}")

    sql_list = sql_list[1:]
    for sql in sql_list:
      token, remaining_sql = next_token(sql)
      token = token.upper()
      if token == "USE":
        db = self.parse_sql_use_db(sql)
      elif token == "CREATE":
        token2, remaining_sql = next_token(remaining_sql)
        token2 = token2.upper()
        if token2 == "TABLE":
          self.parse_sql_create_table(sql, db)
        else:
          raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
      elif token == "INSERT":
        continue
      else:
        raise Exception(f"不合法的sql语句, 仅支持 'USE', 'CREATE TABLE', 'INSERT': {sql}")

  def check_update(self, sql_list: list[str]):
    if len(sql_list) == 0:
      return

    sql = sql_list[0]
    token, remaining_sql = next_token(sql)
    token = token.upper()
    if token != "USE":
      raise Exception(f"init文件中第一条语句必须为 'USE': {sql}")

    db = self.parse_sql_use_db(sql)
    if db is None:
      raise Exception(f"USE Database语法错误: {sql}")

    sql_list = sql_list[1:]
    for sql in sql_list:
      token, remaining_sql = next_token(sql)
      token = token.upper()
      if token == "USE":
        db = self.parse_sql_use_db(sql)
      elif token == "CREATE":
        token2, remaining_sql = next_token(remaining_sql)
        token2 = token2.upper()
        if token2 == "TABLE":
          self.parse_sql_create_table(sql, db)
        else:
          raise Exception(f"不合法的sql语句, 仅支持 'CREATE TABLE': {sql}")
      elif token == "INSERT":
        continue
      elif token == "UPDATE":
        continue
      else:
        raise Exception(f"不合法的sql语句, 仅支持 'USE', 'CREATE TABLE', 'INSERT', 'UPDATE': {sql}")

  def parse_sql_use_db(self, sql: str):
    # USE <db_name>
    remaining_sql = sql
    tokens, remaining_sql = next_tokens(remaining_sql, 2)
    if len(tokens) != 2 or tokens[0].upper() != "USE":
      raise Exception(f"不合法的 USE 语句: {sql}")

    db_name = tokens[1]
    db_name = self.get_real_name(db_name)
    db = Database(db_name)
    return db

  def parse_sql_create_table(self, sql: str, db: Database):
    # 建表语句需以 CREATE TABLE IF NOT EXISTS 开头
    remaining_sql = sql
    tokens, remaining_sql = next_tokens(remaining_sql, 5)
    if (
      len(tokens) != 5
      or tokens[0].upper() != "CREATE"
      or tokens[1].upper() != "TABLE"
      or tokens[2].upper() != "IF"
      or tokens[3].upper() != "NOT"
      or tokens[4].upper() != "EXISTS"
    ):
      raise Exception(f"建表语句需要以 'CREATE TABLE IF NOT EXISTS' 开头: {sql}")

    l_idx = remaining_sql.find("(")
    if l_idx == -1:
      raise Exception(f"不合法的建表语句, 缺少 '(': {sql}")
    table_name = remaining_sql[:l_idx]
    table_name = self.get_real_name(table_name)
    table = Table(table_name)

    r_idx = remaining_sql.rfind(")")
    if r_idx == -1:
      raise Exception(f"不合法的建表语句, 缺少 ')': {sql}")
    columns_define_sql = remaining_sql[l_idx + 1 : r_idx]
    table_define_sql = remaining_sql[r_idx + 1 :].strip(" ;")

    self.parse_sql_table_options(table_define_sql, table)

    columns_sqls = [line.strip(" ,\t") for line in columns_define_sql.splitlines() if line.strip(" ,\t")]
    for column_sql in columns_sqls:
      self.parse_sql_table_struct(column_sql, table)

    self.check_table(table)
    db.add_table(table)

  def parse_sql_table_options(self, sql: str, table: Table):
    # ENGINE = InnoDB AUTO_INCREMENT = 107 DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_bin COMMENT ='活跃度头衔设置';
    if not sql:
      return
    remaining_sql = sql
    while remaining_sql != "":
      key, remaining_sql = next_token(remaining_sql)
      key = key.upper()
      if key == "ENGINE":
        value, remaining_sql = next_token(remaining_sql)
        table.set_options(key, value)
      elif key == "AUTO_INCREMENT":
        value, remaining_sql = next_token(remaining_sql)
        table.set_options(key, value)
      elif key == "COLLATE":
        value, remaining_sql = next_token(remaining_sql)
        table.set_options(key, value)
      elif key == "COMMENT":
        value, remaining_sql = next_token(remaining_sql)
        table.set_options(key, value)
      elif key == "DEFAULT":
        key2, remaining_sql = next_token(remaining_sql)
        key2 = key2.upper()
        if key2 == "CHARSET":
          value, remaining_sql = next_token(remaining_sql)
          table.set_options("DEFAULT CHARSET", value)
        else:
          raise Exception(f"表定义中包含不合法的关键字 '{key2}': {sql}")
      else:
        raise Exception(f"表定义中包含不合法的关键字 '{key}': {sql}")

  def get_real_name(self, name: str):
    real_name = name.strip(" `\n;")
    chars_to_check = {".", '"', "'"}
    if chars_to_check & set(real_name):
      raise Exception(f"名称中包含不合法字符 ('.', '\"', '''): {name}")

    return real_name

  def get_real_column_name(self, name: str):
    real_name = name
    idx = real_name.find("(")
    if idx != -1:
      real_name = real_name[:idx]

    real_name = real_name.strip(" `\n")
    chars_to_check = {".", '"', "'"}
    if chars_to_check & set(real_name):
      raise Exception(f"名称中包含不合法字符 ('.', '\"', '''): {name}")

    return real_name

  def parse_sql_table_struct(self, column_sql: str, table: Table):
    remaining_sql = column_sql
    first_token, remaining_sql = next_token(remaining_sql)
    if first_token == "PRIMARY":
      # PRIMARY KEY (column, ...)
      token, remaining_sql = next_token(remaining_sql)
      if token != "KEY":
        raise Exception(f"主键索引语法错误 :{column_sql}")
      if remaining_sql[0] != "(":
        raise Exception(f"主键索引语法错误 :{column_sql}")
      ridx = remaining_sql.rfind(")")
      if ridx == -1:
        raise Exception(f"主键索引语法错误 :{column_sql}")
      columns_str = remaining_sql[1:ridx]
      columns = [line.strip() for line in columns_str.split(",") if line.strip()]
      index = PrimaryIndex(table.TableName)
      for column in columns:
        column_name = self.get_real_column_name(column)
        index.add_column(column_name)
      table.set_primary_index(index)

    elif first_token == "UNIQUE":
      # UNIQUE KEY uk_index_name (column, ...),
      token, remaining_sql = next_token(remaining_sql)
      if token != "KEY" and token != "INDEX":
        raise Exception(f"唯一索引语法错误 :{column_sql}")
      index_name, remaining_sql = next_token(remaining_sql)
      if remaining_sql == "" or remaining_sql[0] != "(":
        raise Exception(f"唯一索引语法错误 :{column_sql}")
      ridx = remaining_sql.rfind(")")
      if ridx == -1:
        raise Exception(f"唯一索引语法错误 :{column_sql}")
      index_name = self.get_real_name(index_name)
      columns_str = remaining_sql[1:ridx]
      columns = [line.strip() for line in columns_str.split(",") if line.strip()]
      index = UniqueIndex(table.TableName, index_name)
      for column in columns:
        column_name = self.get_real_column_name(column)
        index.add_column(column_name)
      table.add_index(index)

    elif first_token == "KEY" or first_token == "INDEX":
      # KEY idx_index_name (column, ...) 或 INDEX idx_index_name (column, ...)
      index_name, remaining_sql = next_token(remaining_sql)
      if remaining_sql == "" or remaining_sql[0] != "(":
        raise Exception(f"索引语法错误 :{column_sql}")
      ridx = remaining_sql.rfind(")")
      if ridx == -1:
        raise Exception(f"索引语法错误 :{column_sql}")
      index_name = self.get_real_name(index_name)
      columns_str = remaining_sql[1:ridx]
      columns = [line.strip() for line in columns_str.split(",") if line.strip()]
      index = Index(table.TableName, index_name)
      for column in columns:
        column_name = self.get_real_column_name(column)
        index.add_column(column_name)
      table.add_index(index)

    elif first_token == "CONSTRAINT":
      # CONSTRAINT [symbol] FOREIGN KEY constraint_name (column_names) REFERENCES table_name (column_names)
      tokens, remaining_sql = next_tokens(remaining_sql, 3)
      if (tokens[0] == "FOREIGN" and tokens[1] == "KEY") or (tokens[1] == "FOREIGN" and tokens[2] == "KEY"):
        table.add_foreign_key(column_sql)
      else:
        raise Exception(f"约束语法错误 :{column_sql}")

    elif first_token == "FOREIGN":
      # FOREIGN KEY (column) REFERENCES table_name (column)
      token, remaining_sql = next_token(remaining_sql)
      if token != "KEY":
        raise Exception(f"外键约束语法错误 :{column_sql}")
      table.add_foreign_key(column_sql)

    else:
      # column_name column_type unsigned_flag column_identity column_null column_default column_comment
      column_name = first_token
      column = self.parse_sql_column_define(column_name, remaining_sql)
      table.add_column(column)

  def parse_sql_column_define(self, column_name: str, column_sql: str):
    # column_name column_type unsigned_flag column_identity column_null column_default column_comment
    remaining_sql = column_sql
    column_name = self.get_real_column_name(column_name)
    column_type, remaining_sql = next_token(remaining_sql)
    column = Column(column_name, column_type)

    column.ColumnLen, remaining_sql = self.parse_sql_column_len(remaining_sql)
    column.ColumnUnsigned, remaining_sql = self.parse_sql_column_unsigned(remaining_sql)

    while remaining_sql != "":
      key, remaining_sql = next_token(remaining_sql)
      key = key.upper()
      if key == "AUTO_INCREMENT":
        column.ColumnIdentity = key
      elif key == "CHARACTER":
        # CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
        key2, remaining_sql = next_token(remaining_sql)
        if key2.upper() != "SET":
          raise Exception(f"CHARACTER SET 语法错误 :{column_sql}")
        column.ColumnCharset, remaining_sql = next_token(remaining_sql)
      elif key == "COLLATE":
        column.ColumnCollate, remaining_sql = next_token(remaining_sql)
      elif key == "COMMENT":
        column.ColumnComment, remaining_sql = next_token(remaining_sql)
      elif key == "NULL":
        column.ColumnNull = True
      elif key == "NOT":
        key2, remaining_sql = next_token(remaining_sql)
        if key2.upper() == "NULL":
          column.ColumnNull = False
        else:
          raise Exception(f"NOT NULL 语法错误 :{column_sql}")
      elif key == "DEFAULT":
        default_value, remaining_sql = next_token(remaining_sql)
        if remaining_sql.startswith("("):
          # 支持复杂结构，使用栈来匹配对应的右括号
          stack = []
          end_idx = 0
          for i, char in enumerate(remaining_sql):
            if char == "(":
              stack.append(char)
            elif char == ")":
              stack.pop()
              if not stack:
                end_idx = i
                break
          else:
            raise Exception(f"不合法的建表语句, 缺少 ')': {column_sql}")

          default_value += remaining_sql[: end_idx + 1]
          remaining_sql = remaining_sql[end_idx + 1 :].strip(" ")
        column.ColumnDefault = default_value
      else:
        raise Exception(f"列定义中包含不合法的关键字 '{key}': {column_sql}")

    return column

  def parse_sql_column_len(self, column_sql: str):
    if column_sql.startswith("("):
      idx = column_sql.find(")")
      if idx != -1:
        remaining_sql = column_sql[idx + 1 :].strip(" ")
        column_len = column_sql[1:idx].strip(" ")
        return column_len, remaining_sql
    return None, column_sql

  def parse_sql_column_unsigned(self, column_sql: str):
    token, remaining_sql = next_token(column_sql)
    if token.upper().startswith("UNSIGNED"):
      return True, remaining_sql.strip(" ")
    return False, column_sql

  def check_table(self, table: Table):
    if table.PrimaryIndex is None:
      if self.check_config.AllowNonePrimaryKey:
        print(f"[warning] 表 '{table.TableName}' 中缺少主键索引")
      else:
        raise Exception(f"表 '{table.TableName}' 中缺少主键索引")
    else:
      column_names = table.PrimaryIndex.Columns
      for column_name in column_names:
        if column_name not in table.Columns:
          raise Exception(f"表 '{table.TableName}' 中不存在主键索引中的字段 '{column_name}'")

    for index_name in table.Indices:
      index = table.Indices[index_name]
      column_names = index.Columns
      for column_name in column_names:
        if column_name not in table.Columns:
          raise Exception(f"表 '{table.TableName}' 中不存在索引 '{index_name}' 中的字段 '{column_name}'")

    for column_name in table.Columns:
      column = table.Columns[column_name]
      self.check_column(table.TableName, column)

    if len(table.ForeignKeys) > 0:
      if self.check_config.AllowForeignKey:
        print(f"[warning] 表 '{table.TableName}' 中存在外键约束")
      else:
        raise Exception(f"表 '{table.TableName}' 中存在外键约束, 但配置中不允许外键约束")

  def check_column(self, table_name: str, column: Column):
    if column.ColumnType in ("TEXT", "MEDIUMTEXT", "LONGTEXT", "BLOB", "JSON"):
      if column.ColumnDefault is not None and column.ColumnDefault.upper() != "NULL":
        raise Exception(f"表 '{table_name}' 中字段 '{column.ColumnName}' 是文本类型字段, 不支持设置默认值")
    elif column.ColumnType == "TINYINT" and column.ColumnLen == "1":
      raise Exception(f"表 '{table_name}' 中字段 '{column.ColumnName}' 类型是tinyint(1), 不支持该类型, 请使用BOOLEAN")
