#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import sqlparse

from rds.rds import CheckRDS
from rds.mariadb import CheckMariaDB
from rds.dm8 import CheckDM8
from rds.kdb9 import CheckKDB9
from utils.check_config import CheckConfig
from utils.version import VersionUtil


def check_repos(config):
  print("\n开始检查代码库目录")

  check_config = CheckConfig(config)

  has_error = False
  services = config["services"]
  for service_name, service_config in services.items():
    print(f"\n开始检查代码库目录: {service_name}")
    repo_path = os.path.join(os.getcwd(), "repos", service_name)
    check_from = service_config.get("check_from", None)
    if not check_repo(repo_path, check_config, check_from):
      has_error = True

  if has_error:
    raise Exception("代码库目录检查失败")

  print("\n代码库目录检查成功")


def check_repo(repo_path: str, check_config: CheckConfig, check_from: str = None):
  print(f"\nrepo目录: {repo_path}")

  for db_type in check_config.DBTypes:
    if db_type == "dm8":
      check_rds = CheckDM8(check_config)
    elif db_type == "mariadb":
      check_rds = CheckMariaDB(check_config)
    elif db_type == "kdb9":
      check_rds = CheckKDB9(check_config)
    else:
      raise Exception(f"不支持的数据库类型: {db_type}")

    repo_db_path = os.path.join(repo_path, db_type)

    try:
      check_db_type(repo_db_path, check_rds, check_from)
    except Exception as e:
      print(f"[error]: check_db_type 失败: {repo_db_path}, 错误信息: {e}")
      return False

  return True


def check_db_type(repo_db_path: str, check_rds: CheckRDS, check_from: str = None):
  print(f"db目录: {repo_db_path}")
  version_list = os.listdir(repo_db_path)
  versions = []
  for version in version_list:
    versions.append(VersionUtil(version))
  versions.sort()

  # 如果指定了 check_from，筛选出 >= check_from 的版本
  if check_from:
    from_version = VersionUtil(check_from)
    versions = [v for v in versions if v >= from_version]
    print(f"从版本 {check_from} 开始检测, 筛选后待检查的目录: {versions}")

  if check_rds.check_config.CheckType == CheckConfig.CheckLatest:
    if len(versions) >= 1:
      versions = versions[-1:]
    print(f"检查最新一次, 待检查的目录: {versions}")
  elif check_rds.check_config.CheckType == CheckConfig.CheckRecently:
    if len(versions) >= 2:
      versions = versions[-2:]
    print(f"检查最近两次, 待检查的目录: {versions}")
  elif check_rds.check_config.CheckType == CheckConfig.CheckAll:
    print(f"检查全部, 待检查的目录: {versions}")

  for version in versions:
    version_dir = os.path.join(repo_db_path, version.VersionStr)
    check_version(version_dir, check_rds)


def check_version(version_dir: str, check_rds: CheckRDS):
  print(f"version目录: {version_dir}")
  filenames = os.listdir(version_dir)
  if len(filenames) <= 0:
    raise Exception(f"空目录: {version_dir}")

  for filename in filenames:
    if filename != "pre" and filename != "post":
      invalid_path = os.path.join(version_dir, filename)
      raise Exception(f"无效目录名, 仅支持 'pre', 'post': {invalid_path}")

  if "pre" in filenames:
    version_pre_dir = os.path.join(version_dir, "pre")
    check_version_pre(version_pre_dir, check_rds)
  else:
    raise Exception(f"pre目录不存在: {version_dir}")

  if "post" in filenames:
    version_post_dir = os.path.join(version_dir, "post")
    check_version_post(version_post_dir, check_rds)


def check_version_pre(version_pre_dir: str, check_rds: CheckRDS):
  print(f"pre目录: {version_pre_dir}")
  filenames = os.listdir(version_pre_dir)
  if len(filenames) <= 0:
    raise Exception(f"空目录: {version_pre_dir}")

  found_init = False
  update_files = {}
  for filename in filenames:
    if filename == "init.sql":
      found_init = True
    else:
      if len(filename) >= 3 and filename[:2].isdigit() and filename[2] == "-":
        if filename.endswith(".py") or filename.endswith(".sql") or filename.endswith(".json"):
          pre_file = os.path.join(version_pre_dir, filename)
          seq = int(filename[:2])
          if seq not in update_files:
            update_files[seq] = pre_file
          else:
            raise Exception(f"重复的文件序号: {update_files[seq]}, {pre_file}")
        else:
          raise Exception(f"无效的升级文件: {filename}")

  if found_init:
    init_file = os.path.join(version_pre_dir, "init.sql")
    check_init_file(init_file, check_rds)

  sorted_seqs = sorted(update_files)
  for seq in sorted_seqs:
    update_file = os.path.join(version_pre_dir, update_files[seq])
    check_update_file(update_file, check_rds)


def check_version_post(version_post_dir: str, check_rds: CheckRDS):
  print(f"post目录: {version_post_dir}")
  filenames = os.listdir(version_post_dir)
  if len(filenames) <= 0:
    return

  update_files = {}
  for filename in filenames:
    if len(filename) >= 3 and filename[:2].isdigit() and filename[2] == "-":
      if filename.endswith(".py"):
        post_file = os.path.join(version_post_dir, filename)
        seq = int(filename[:2])
        if seq not in update_files:
          update_files[seq] = post_file
        else:
          raise Exception(f"重复的文件序号: {update_files[seq]}, {post_file}")
      else:
        raise Exception(f"无效的文件类型, post仅支持python文件: {post_file}")

  sorted_seqs = sorted(update_files)
  for seq in sorted_seqs:
    update_file = os.path.join(version_post_dir, update_files[seq])
    check_update_file(update_file, check_rds)


def check_init_file(init_file: str, check_rds: CheckRDS):
  """
  检查init文件是否合法

  Args:
    init_file: 要检查的init文件路径
  """
  print(f"检查init文件: {init_file}")
  with open(init_file, "r", encoding="utf-8") as f:
    sqls_str = f.read()
  formated_sqls_str = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
  sql_list = sqlparse.split(formated_sqls_str)
  sql_list = [sql for sql in sql_list if sql.strip() != ";"]
  if len(sql_list) == 0:
    return

  check_rds.check_init(sql_list)


def check_update_file(update_file: str, check_rds: CheckRDS):
  print(f"检查升级文件: {update_file}")
  if update_file.endswith(".json"):
    check_update_file_json(update_file, check_rds)
  elif update_file.endswith(".sql"):
    check_update_file_sql(update_file, check_rds)
  elif update_file.endswith(".py"):
    pass
  else:
    raise Exception(f"不支持的升级文件类型: {update_file}")
  print(f"检查通过: {update_file}")


def check_update_file_json(update_file: str, check_rds: CheckRDS):
  with open(update_file, "r", encoding="utf-8") as f:
    try:
      update_items = json.load(f)
    except json.JSONDecodeError as e:
      raise Exception(f"无效的JSON文件: {update_file}, {e}")
    if not isinstance(update_items, list):
      raise Exception(f"JSON根类型必须为对象(list): {update_file}")

  required_fields = [
    "db_name",
    "table_name",
    "object_type",
    "operation_type",
    "object_name",
    "object_property",
    "object_comment",
  ]

  # 操作对象包含 COLUMN、INDEX、UNIQUE INDEX
  # 对象名对应操作对象的名称，如果操作对象为 COLUMN，则为字段名；如果操作对象为 INDEX/UNIQUE INDEX，则为索引名
  # 操作类型包含 ADD、DROP、MODIFY
  # 对象属性如果是 COLUMN，包含字段类型、是否为空、默认值；如果是 INDEX/UNIQUE INDEX，包含索引列(联合索引列之间用逗号分隔)、排序方式
  # 对象属性、字段注释没有时填空字符串
  # 特例：删除表，只需数据库名，表名，操作对象为TABLE，操作类型为DROP，其他全填空字符串
  # 特例：删除库，只需数据库名，操作对象为DB，，操作类型为DROP，其他全填空字符串

  allowed_object_types = {"COLUMN", "INDEX", "UNIQUE INDEX", "CONSTRAINT", "TABLE", "DB"}
  allowed_operation_types = {"ADD", "DROP", "MODIFY", "RENAME"}

  if len(update_items) <= 0:
    raise Exception(f"JSON列表不能为空: {update_file}")

  for item in update_items:
    if not isinstance(item, dict):
      raise Exception(f"格式错误: {item}")

    # 必填字段存在性与类型校验
    for field in required_fields:
      if field not in item:
        raise Exception(f"缺少必填字段 '{field}': {item}")
      if not isinstance(item[field], str):
        raise Exception(f"字段 '{field}' 必须为字符串: {item}")

    table_name = item["table_name"]
    object_name = item["object_name"]
    object_type = item["object_type"]
    object_property = item["object_property"]
    operation_type = item["operation_type"]

    # 取值范围校验
    if object_type not in allowed_object_types:
      raise Exception(f"不支持的 object_type '{object_type}': {item}")

    if operation_type not in allowed_operation_types:
      raise Exception(f"不支持的 operation_type '{operation_type}': {item}")

    if object_type == "COLUMN" and operation_type == "ADD":
      column = check_rds.parse_sql_column_define(object_name, object_property)
      if column is None:
        raise Exception(f"无效的列定义: {object_name}, {object_property}")
      check_rds.check_column(table_name, column)


def check_update_file_sql(update_file: str, check_rds: CheckRDS):
  print(f"检查update sql文件: {update_file}")
  with open(update_file, "r", encoding="utf-8") as f:
    sqls_str = f.read()
  formated_sqls_str = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
  sql_list = sqlparse.split(formated_sqls_str)
  if len(sql_list) == 0:
    return

  check_rds.check_update(sql_list)
