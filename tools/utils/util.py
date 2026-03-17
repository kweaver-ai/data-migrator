#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import shutil

from utils.version import is_version_dir


def copy_files(source_dir, dest_dir):
  """实现，复制某个目录下所有文件到，指定目录下"""

  # 遍历源目录中的所有文件
  for file_name in os.listdir(source_dir):
    if not is_version_dir(file_name):
      print(f"{file_name} 不是一个版本文件夹，跳过复制")
      continue

    source_file = os.path.join(source_dir, file_name)
    dest_file = os.path.join(dest_dir, file_name)

    # 复制文件到目标目录
    shutil.copytree(source_file, dest_file, dirs_exist_ok=True)


def next_tokens(sql: str, size: int):
  tokens = []
  remaining_sql = sql
  i = 0
  while i < size and remaining_sql != "":
    i+=1
    token, remaining_sql = next_token(remaining_sql)
    tokens.append(token)

  return tokens, remaining_sql


def next_token(sql: str):
  token = ""
  remaining_sql = ""

  new_sql = sql.strip(" =\n")
  if new_sql == "":
    return token, remaining_sql

  c = new_sql[0]
  if c == '`' or c == '\'' or c == '\"':
    new_sql = new_sql[1:]
    idx = new_sql.find(c)
    if idx == -1:
      raise Exception(f"sql语句解析token错误: {sql}")
    else:
      token = new_sql[:idx]
      remaining_sql = new_sql[idx+1:].strip(' =')
  else:
    for idx, char in enumerate(new_sql):
      if char in (' ', '=', '('):
        token = sql[:idx]
        remaining_sql = sql[idx:].strip(' =')
        break
    else:
      token = new_sql
      remaining_sql = ''

  return token, remaining_sql
