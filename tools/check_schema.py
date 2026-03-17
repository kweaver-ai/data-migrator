#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
import sqlparse
import subprocess
import sys

from rds.rds import CheckRDS
from rds.mariadb import CheckMariaDB
from rds.dm8 import CheckDM8
from rds.kdb9 import CheckKDB9
from utils.check_config import CheckConfig
from utils.version import VersionUtil


def check_repos(config):
  print("\n开始验证数据模型脚本")

  check_config = CheckConfig(config)
  reset_schema(check_config)

  services = config["services"]
  for service_name, service_config in services.items():
    print(f"\n开始验证数据模型目录: {service_name}")
    repo_path = os.path.join(os.getcwd(), "repos", service_name)
    check_from = service_config.get("check_from", None)
    if not check_repo(repo_path, check_config, check_from):
      raise Exception(f"数据模型 {service_name} 验证失败")

  print("\n数据模型验证成功")


def reset_schema(check_config: CheckConfig):
  print("重置数据模式")
  for db_type in check_config.DBTypes:
    if db_type == "dm8":
      primary_rds_check = CheckDM8(check_config, is_primary=True)
      secondary_rds_check = CheckDM8(check_config, is_primary=False)
    elif db_type == "mariadb":
      primary_rds_check = CheckMariaDB(check_config, is_primary=True)
      secondary_rds_check = CheckMariaDB(check_config, is_primary=False)
    elif db_type == "kdb9":
      primary_rds_check = CheckKDB9(check_config, is_primary=True)
      secondary_rds_check = CheckKDB9(check_config, is_primary=False)
    else:
      raise Exception(f"不支持的数据库类型: {db_type}")

    try:
      primary_rds_check.reset_schema(check_config.DATABASES)
      secondary_rds_check.reset_schema(check_config.DATABASES)
    except Exception as e:
      print(f"[error]: reset_schema 失败: {db_type}, 错误信息: {e}")
      raise Exception(f"reset_schema 失败: {db_type}")


def check_repo(repo_path: str, check_config: CheckConfig, check_from: str = None):
  print(f"\nrepo目录: {repo_path}")

  base_rds_check = None
  for db_type in check_config.DBTypes:
    if db_type == "dm8":
      primary_rds_check = CheckDM8(check_config, is_primary=True)
      secondary_rds_check = CheckDM8(check_config, is_primary=False)
    elif db_type == "mariadb":
      primary_rds_check = CheckMariaDB(check_config, is_primary=True)
      secondary_rds_check = CheckMariaDB(check_config, is_primary=False)
    elif db_type == "kdb9":
      primary_rds_check = CheckKDB9(check_config, is_primary=True)
      secondary_rds_check = CheckKDB9(check_config, is_primary=False)
    else:
      raise Exception(f"不支持的数据库类型: {db_type}")

    repo_db_path = os.path.join(repo_path, db_type)

    try:
      check_db_type(repo_db_path, primary_rds_check, secondary_rds_check, check_from)
    except Exception as e:
      print(f"[error]: check_db_type 失败: {repo_db_path}, 错误信息: {e}")
      return False

    if base_rds_check is None:
      base_rds_check = primary_rds_check
    else:
      compare_schema(check_config, base_rds_check, primary_rds_check)

  return True


def check_db_type(repo_db_path: str, primary_rds_check: CheckRDS, secondary_rds_check: CheckRDS, check_from: str = None):
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

  if primary_rds_check.check_config.CheckType == CheckConfig.CheckLatest:
    if len(versions) >= 1:
      versions = versions[-1:]
    print(f"检查最新一次, 待检查的目录: {versions}")
  elif primary_rds_check.check_config.CheckType == CheckConfig.CheckRecently:
    if len(versions) >= 2:
      versions = versions[-2:]
    print(f"检查最近两次, 待检查的目录: {versions}")
  elif primary_rds_check.check_config.CheckType == CheckConfig.CheckAll:
    print(f"检查全部, 待检查的目录: {versions}")

  for version in versions:
    version_dir = os.path.join(repo_db_path, version.VersionStr)
    if versions.index(version) == 0:
      check_version_init(version_dir, primary_rds_check)
      check_version(version_dir, primary_rds_check, check_pre=False, check_post=True)
    else:
      check_version(version_dir, primary_rds_check, check_pre=True, check_post=True)

  # 对比最新一次和次新一次的差异
  if len(versions) >= 2:
    version_dir = os.path.join(repo_db_path, versions[-1].VersionStr)
    check_version_init(version_dir, secondary_rds_check)
    check_version(version_dir, secondary_rds_check, check_pre=False, check_post=True)
    compare_schema(primary_rds_check.check_config, primary_rds_check, secondary_rds_check)
  else:
    version_dir = os.path.join(repo_db_path, versions[0].VersionStr)
    check_version_init(version_dir, secondary_rds_check)


def check_version_init(version_dir: str, check_rds: CheckRDS):
  print(f"version目录: {version_dir}")
  filenames = os.listdir(version_dir)

  if "pre" not in filenames:
    raise Exception(f"pre目录不存在: {version_dir}")

  version_pre_dir = os.path.join(version_dir, "pre")
  filenames = os.listdir(version_pre_dir)
  if "init.sql" not in filenames:
    raise Exception(f"init.sql 文件不存在: {version_pre_dir}")

  init_file = os.path.join(version_pre_dir, "init.sql")
  check_init_file(init_file, check_rds)


def check_version(version_dir: str, check_rds: CheckRDS, check_pre: bool = True, check_post: bool = True):
  filenames = os.listdir(version_dir)
  if len(filenames) <= 0:
    raise Exception(f"空目录: {version_dir}")

  for filename in filenames:
    if filename != "pre" and filename != "post":
      invalid_path = os.path.join(version_dir, filename)
      raise Exception(f"无效目录名, 仅支持 'pre', 'post': {invalid_path}")

  if check_pre:
    if "pre" in filenames:
      version_pre_dir = os.path.join(version_dir, "pre")
      check_version_pre(version_pre_dir, check_rds)
    else:
      raise Exception(f"pre目录不存在: {version_dir}")

  if check_post:
    if "post" in filenames:
      version_post_dir = os.path.join(version_dir, "post")
      check_version_post(version_post_dir, check_rds)


def check_version_pre(version_pre_dir: str, check_rds: CheckRDS):
  print(f"pre目录: {version_pre_dir}")
  filenames = os.listdir(version_pre_dir)
  if len(filenames) <= 0:
    raise Exception(f"空目录: {version_pre_dir}")

  update_files = {}
  for filename in filenames:
    if filename == "init.sql":
      continue

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

  sorted_seqs = sorted(update_files)
  for seq in sorted_seqs:
    update_file = os.path.join(version_pre_dir, update_files[seq])
    try:
      check_update_file(update_file, check_rds)
    except Exception as e:
      print(f"[error]: check_update_file 失败: {update_file}, 错误信息: {e}")
      if not check_rds.check_config.AllowPythonException:
        raise Exception(f"check_update_file 失败: {update_file}")


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
    try:
      check_update_file(update_file, check_rds)
    except Exception as e:
      print(f"[error]: check_update_file 失败: {update_file}, 错误信息: {e}")
      if not check_rds.check_config.AllowPythonException:
        raise Exception(f"check_update_file 失败: {update_file}")


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

  check_rds.run_sql(sql_list)


def check_update_file(update_file: str, check_rds: CheckRDS):
  print(f"检查升级文件: {update_file}")
  if update_file.endswith(".json"):
    check_update_file_json(update_file, check_rds)
  elif update_file.endswith(".sql"):
    check_update_file_sql(update_file, check_rds)
  elif update_file.endswith(".py"):
    check_update_file_py(update_file, check_rds)
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

  # 操作对象包含 COLUMN、INDEX、UNIQUE INDEX、CONSTRAINT、TABLE、DB
  # 对象名对应操作对象的名称，如果操作对象为 COLUMN，则为字段名；如果操作对象为 INDEX/UNIQUE INDEX，则为索引名,
  #   如果操作对象为 TABLE，则为表名；如果操作对象为 DB，则为数据库名
  # 操作类型包含 ADD、DROP、MODIFY, RENAME
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

    db_name = item["db_name"]
    table_name = item["table_name"]
    object_type = item["object_type"]
    operation_type = item["operation_type"]
    object_name = item.get("object_name", "")
    new_name = item.get("new_name", "")
    object_property = item.get("object_property", "")
    object_comment = item.get("object_comment", "")

    # 取值范围校验
    if object_type not in allowed_object_types:
      raise Exception(f"不支持的 object_type '{object_type}': {item}")

    if operation_type not in allowed_operation_types:
      raise Exception(f"不支持的 operation_type '{operation_type}': {item}")

    if object_type == "COLUMN":
      if operation_type == "ADD":
        check_rds.add_column(db_name, table_name, object_name, object_property, object_comment)
      elif operation_type == "MODIFY":
        check_rds.modify_column(db_name, table_name, object_name, object_property, object_comment)
      elif operation_type == "RENAME":
        check_rds.rename_column(db_name, table_name, object_name, new_name, object_property, object_comment)
      elif operation_type == "DROP":
        check_rds.drop_column(db_name, table_name, object_name)
      else:
        raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
    elif object_type == "INDEX" or object_type == "UNIQUE INDEX":
      if operation_type == "ADD":
        check_rds.add_index(db_name, table_name, object_type, object_name, object_property, object_comment)
      elif operation_type == "RENAME":
        check_rds.rename_index(db_name, table_name, object_name, new_name)
      elif operation_type == "DROP":
        check_rds.drop_index(db_name, table_name, object_name)
      else:
        raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
    elif object_type == "CONSTRAINT":
      if operation_type == "ADD":
        check_rds.add_constraint(db_name, table_name, object_name, object_property)
      elif operation_type == "RENAME":
        check_rds.rename_constraint(db_name, table_name, object_name, new_name)
      elif operation_type == "DROP":
        check_rds.drop_constraint(db_name, table_name, object_name)
      else:
        raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
    elif object_type == "TABLE":
      if operation_type == "RENAME":
        check_rds.rename_table(db_name, table_name, new_name)
      elif operation_type == "DROP":
        check_rds.drop_table(db_name, table_name)
      else:
        raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
    elif object_type == "DB":
      if operation_type == "DROP":
        check_rds.drop_db(db_name)
      else:
        raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")


def check_update_file_sql(update_file: str, check_rds: CheckRDS):
  print(f"检查update sql文件: {update_file}")
  with open(update_file, "r", encoding="utf-8") as f:
    sqls_str = f.read()
  formated_sqls_str = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
  sql_list = sqlparse.split(formated_sqls_str)
  if len(sql_list) == 0:
    return

  check_rds.run_sql(sql_list)


def check_update_file_py(update_file: str, check_rds: CheckRDS):
  print(f"检查update py文件: {update_file}")
  try:
    # 运行 Python 文件
    venv_python = sys.executable

    custom_env = os.environ.copy()
    custom_env["CI_MODE"] = "true"
    custom_env["PYTHONUNBUFFERED"] = "1"

    custom_env["DB_TYPE"] = check_rds.DB_TYPE
    custom_env["DB_HOST"] = check_rds.DB_CONFIG_ROOT["host"]
    custom_env["DB_PORT"] = str(check_rds.DB_CONFIG_ROOT["port"])
    custom_env["DB_USER"] = check_rds.DB_CONFIG_ROOT["user"]
    custom_env["DB_PASSWD"] = check_rds.DB_CONFIG_ROOT["password"]

    result = subprocess.run([venv_python, update_file], env=custom_env, capture_output=True, text=True, check=True, encoding="utf-8")
    print(f"运行 {update_file} 成功, result: {result}")
  except subprocess.CalledProcessError as e:
    print(f"[error] 运行 Python 文件失败: {update_file}, 错误信息: {e.stderr}")
    raise Exception(f"运行 Python 文件失败: {update_file}")


def compare_schema(check_config: CheckConfig, base_rds: CheckRDS, check_rds: CheckRDS):
  """
  对比两个数据库的schema差异

  Args:
    base_rds: 基准数据库
    check_rds: 要对比的数据库
  """
  print(f"\n对比数据库schema差异: {base_rds.DB_TYPE} -> {check_rds.DB_TYPE}")

  for db_name in check_config.DATABASES:
    base_table_list = base_rds.list_tables_by_db(db_name)
    check_table_list = check_rds.list_tables_by_db(db_name)

    diff_table_list = set(check_table_list) - set(base_table_list)
    if len(diff_table_list) > 0:
      print(f"[error] 对比数据库的表中存在基准数据库中不存在的表: {diff_table_list}")
      print(f"基准数据库的表: {base_table_list}")
      print(f"对比数据库的表: {check_table_list}")
      raise Exception(f"数据库 {db_name} 表数量不一致")

    if not check_config.AllowTableCompareDismatch:
      diff_table_list = set(base_table_list) - set(check_table_list)
      if len(diff_table_list) > 0:
        print(f"[error] 基准数据库的表中存在对比数据库中不存在的表: {diff_table_list}")
        print(f"基准数据库的表: {base_table_list}")
        print(f"对比数据库的表: {check_table_list}")
        raise Exception(f"数据库 {db_name} 表数量不一致")

    for table_name in base_table_list:
      if table_name not in check_table_list:
        print(f"[warning] 数据库 {db_name} 表 {table_name} 在对比数据库中不存在")
        continue

      base_table_columns = base_rds.get_table_columns(db_name, table_name)
      check_table_columns = check_rds.get_table_columns(db_name, table_name)
      if len(base_table_columns) != len(check_table_columns):
        print(f"[error] 数据库 {db_name} 表 {table_name} 列数量不一致")
        print(f"基准数据库的列: {base_table_columns}")
        print(f"对比数据库的列: {check_table_columns}")
        raise Exception(f"数据库 {db_name} 表 {table_name} 列数量不一致")

      for column in base_table_columns:
        if column not in check_table_columns:
          print(f"[error] 数据库 {db_name} 表 {table_name} 列 {column} 不存在")
          raise Exception(f"数据库 {db_name} 表 {table_name} 列 {column} 不存在")

        base_column = base_table_columns[column]
        check_column = check_table_columns[column]
        base_data_type, base_data_category = base_rds.get_column_type(base_column)
        check_data_type, check_data_category = check_rds.get_column_type(check_column)
        if base_data_category != check_data_category:
          print(
            f"[warning] 数据库 {db_name} 表 {table_name} 列 {column} 数据类型不一致,"
            f"{base_rds.DB_TYPE}: {base_data_type} -> {check_rds.DB_TYPE}: {check_data_type}"
          )

  print("对比数据库schema差异结束")
