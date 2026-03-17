#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re

from utils.check_config import CheckConfig


class Column():
  def __init__(self, colun_name: str, column_type: str):
    self.ColumnName = colun_name
    self.ColumnType = column_type.upper()
    self.ColumnLen = None
    self.ColumnUnsigned = None
    self.ColumnIdentity = None
    self.ColumnCharset = None
    self.ColumnCollate = None
    self.ColumnNull = None
    self.ColumnDefault = None
    self.ColumnComment = None


class PrimaryIndex():
  def __init__(self, table_name: str):
    self.TableName = table_name
    self.Columns = {}

  def add_column(self, column: str):
    if column in self.Columns:
      raise Exception(f"表{self.TableName} 的主键索引中的字段 '{column}' 重复")
    self.Columns[column] = column


class Index:
  def __init__(self, table_name: str, index_name: str):
    pattern = r'^idx_[a-z0-9_]{1,60}$'
    if not re.fullmatch(pattern, index_name):
      print(f"[warning] 索引名 '{index_name}' 需以 'idx_' 开头, 仅支持小写字母, 数字, 下划线, "
        "长度不超过64, 建议不超过20")
    self.TableName = table_name
    self.IndexName = index_name
    self.Columns = {}

  def add_column(self, column: str):
    # 检查如果column为数字字符串，则pass
    if column.isdigit():
      return

    if column in self.Columns:
      raise Exception(f"表{self.TableName} 的索引 '{self.IndexName}' 中的字段 '{column}' 重复")
    self.Columns[column] = column


class UniqueIndex(Index):
  def __init__(self, table_name: str, index_name: str):
    pattern = r'^uk_[a-z0-9_]{1,61}$'
    if not re.fullmatch(pattern, index_name):
      print(f"[warning] 唯一索引名 '{index_name}' 需以 'uk_' 开头, 仅支持小写字母, 数字, 下划线, "
        "长度不超过64, 建议不超过20")

    self.TableName = table_name
    self.IndexName = index_name
    self.Columns = {}


class Table:
  def __init__(self, table_name: str):
    # 表名需以 t_ 开头，只能包含小写字母、数字、_, 总长度不超过64
    pattern = r'^t_[a-z0-9_]{1,62}$'
    if not re.fullmatch(pattern, table_name):
      print(f"[warning] 表名 '{table_name}' 需以 't_' 开头, 仅支持小写字母, 数字, 下划线, "
        "长度不超过64, 建议不超过20")

    self.TableName = table_name
    self.TableOptions = {}
    self.Columns = {}
    self.PrimaryIndex = None
    self.Indices = {}
    self.ForeignKeys = []

  def add_column(self, column: Column):
    if column.ColumnName in self.Columns:
      raise Exception(f"表 '{self.TableName}' 中的字段 '{column.ColumnName}' 重复")
    self.Columns[column.ColumnName] = column

  def set_primary_index(self, index: PrimaryIndex):
    if self.PrimaryIndex is not None:
      raise Exception(f"表 '{self.TableName}' 中的主键索引重复")
    self.PrimaryIndex = index

  def add_index(self, index: Index):
    if index.IndexName in self.Indices:
      raise Exception(f"表 '{self.TableName}' 中的索引 '{index.IndexName}' 重复")
    self.Indices[index.IndexName] = index

  def set_options(self, key, value):
    if key in self.TableOptions:
      raise Exception(f"表 '{self.TableName}' 属性 '{key}' 重复, 原值: {self.TableOptions[key]}, 新值: {value}")
    self.TableOptions[key] = value

  def add_foreign_key(self, foreign_key: str):
    self.ForeignKeys.append(foreign_key)


class Database:
  def __init__(self, db_name: str):
    self.DBName = db_name
    self.Tables = {}

  def add_table(self, table: Table):
    if table.TableName in self.Tables:
      raise Exception(f"表 '{table.TableName}' 在库 '{self.DBName}' 中已存在")
    self.Tables[table.TableName] = table

  def get_table(self, table_name: str):
    if table_name in self.Tables:
      return self.Tables[table_name]
    return None
