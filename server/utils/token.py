#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQL token 解析工具 — 移植自 tools/utils/util.py"""


def next_tokens(sql: str, size: int):
  tokens = []
  remaining_sql = sql
  i = 0
  while i < size and remaining_sql != "":
    i += 1
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
      remaining_sql = new_sql[idx + 1:].strip(' =')
  else:
    for idx, char in enumerate(new_sql):
      if char in (' ', '=', '('):
        token = new_sql[:idx]
        remaining_sql = new_sql[idx:].strip(' =')
        break
    else:
      token = new_sql
      remaining_sql = ''

  return token, remaining_sql
