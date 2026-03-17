#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SQL 执行校验 - 复用 tools/check_schema.py 逻辑，适配 AppConfig

在 CI 环境中连接测试数据库，实际执行 init.sql 和升级脚本，校验语法正确性。
"""
import json
import os
import subprocess
import sys
from logging import Logger

import rdsdriver
import sqlparse

from server.config.models import AppConfig
from server.check.check_config import CheckConfig
from server.check.rds.base import CheckRDS
from server.check.rds.mariadb import CheckMariaDB
from server.check.rds.dm8 import CheckDM8
from server.check.rds.kdb9 import CheckKDB9
from server.utils.version import VersionUtil


def run_check_schema(app_config: AppConfig, logger: Logger):
    """SQL 执行校验主入口"""
    logger.info("开始验证数据模型脚本")

    check_config = CheckConfig(app_config)
    _reset_schema(check_config, logger)

    for service_name, service_cfg in app_config.services.items():
        logger.info(f"验证服务: {service_name}")
        repo_path = os.path.join(os.getcwd(), "repos", service_name)
        check_from = service_cfg.check_from
        if not _check_repo(repo_path, check_config, check_from, logger):
            raise Exception(f"数据模型 {service_name} 验证失败")

    logger.info("数据模型验证成功")


def _create_check_rds(db_type: str, check_config: CheckConfig,
                      is_primary: bool = True) -> CheckRDS:
    if db_type == "dm8":
        return CheckDM8(check_config, is_primary=is_primary)
    elif db_type == "mariadb":
        return CheckMariaDB(check_config, is_primary=is_primary)
    elif db_type == "kdb9":
        return CheckKDB9(check_config, is_primary=is_primary)
    else:
        raise Exception(f"不支持的数据库类型: {db_type}")


def _reset_schema(check_config: CheckConfig, logger: Logger):
    logger.info("重置数据模式")
    for db_type in check_config.DBTypes:
        primary = _create_check_rds(db_type, check_config, is_primary=True)
        secondary = _create_check_rds(db_type, check_config, is_primary=False)
        try:
            primary.reset_schema(check_config.DATABASES)
            secondary.reset_schema(check_config.DATABASES)
        except Exception as e:
            logger.error(f"reset_schema 失败: {db_type}, 错误: {e}")
            raise


def _check_repo(repo_path: str, check_config: CheckConfig,
                check_from: str, logger: Logger) -> bool:
    base_rds = None
    for db_type in check_config.DBTypes:
        primary = _create_check_rds(db_type, check_config, is_primary=True)
        secondary = _create_check_rds(db_type, check_config, is_primary=False)

        repo_db_path = os.path.join(repo_path, db_type)
        try:
            _check_db_type(repo_db_path, primary, secondary, check_from, logger)
        except Exception as e:
            logger.error(f"check_db_type 失败: {repo_db_path}, 错误: {e}")
            return False

        if base_rds is None:
            base_rds = primary
        else:
            _compare_schema(check_config, base_rds, primary, logger)

    return True


def _check_db_type(repo_db_path: str, primary: CheckRDS, secondary: CheckRDS,
                   check_from: str, logger: Logger):
    """新结构：脚本在 <version>/ 下，无 pre/post"""
    version_list = os.listdir(repo_db_path)
    versions = []
    for v in version_list:
        try:
            versions.append(VersionUtil(v))
        except (ValueError, AttributeError):
            continue
    versions.sort()

    if check_from:
        from_version = VersionUtil(check_from)
        versions = [v for v in versions if v >= from_version]

    if primary.check_config.CheckType == CheckConfig.CheckLatest:
        if len(versions) >= 1:
            versions = versions[-1:]
    elif primary.check_config.CheckType == CheckConfig.CheckRecently:
        if len(versions) >= 2:
            versions = versions[-2:]

    for i, version in enumerate(versions):
        version_dir = os.path.join(repo_db_path, version.VersionStr)
        if i == 0:
            # 第一个版本：执行 init.sql
            _check_version_init(version_dir, primary, logger)
        # 执行增量脚本
        _check_version_upgrades(version_dir, primary, logger)

    # 对比最新一次 init
    if len(versions) >= 1:
        last_dir = os.path.join(repo_db_path, versions[-1].VersionStr)
        _check_version_init(last_dir, secondary, logger)


def _check_version_init(version_dir: str, check_rds: CheckRDS, logger: Logger):
    """执行 init.sql"""
    init_file = os.path.join(version_dir, "init.sql")
    if not os.path.isfile(init_file):
        return

    logger.info(f"执行 init.sql: {init_file}")
    with open(init_file, "r", encoding="utf-8") as f:
        sqls_str = f.read()
    formatted = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
    sql_list = sqlparse.split(formatted)
    sql_list = [sql for sql in sql_list if sql.strip() and sql.strip() != ";"]
    if sql_list:
        check_rds.run_sql(sql_list)


def _check_version_upgrades(version_dir: str, check_rds: CheckRDS, logger: Logger):
    """执行版本目录下的增量脚本"""
    filenames = sorted(os.listdir(version_dir))
    for filename in filenames:
        if filename == "init.sql":
            continue
        filepath = os.path.join(version_dir, filename)
        if not os.path.isfile(filepath):
            continue

        if filename.endswith(".json"):
            logger.warning(f"跳过 .json 文件: {filepath}")
            continue

        if filename.endswith(".sql"):
            _check_sql_file(filepath, check_rds, logger)
        elif filename.endswith(".py"):
            _check_py_file(filepath, check_rds, logger)


def _check_sql_file(filepath: str, check_rds: CheckRDS, logger: Logger):
    logger.info(f"执行 SQL 文件: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        sqls_str = f.read()
    formatted = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
    sql_list = sqlparse.split(formatted)
    sql_list = [sql for sql in sql_list if sql.strip() and sql.strip() != ";"]
    if sql_list:
        check_rds.run_sql(sql_list)


def _check_py_file(filepath: str, check_rds: CheckRDS, logger: Logger):
    logger.info(f"执行 Python 文件: {filepath}")
    try:
        custom_env = os.environ.copy()
        custom_env["CI_MODE"] = "true"
        custom_env["PYTHONUNBUFFERED"] = "1"
        custom_env["DB_TYPE"] = check_rds.DB_TYPE
        custom_env["DB_HOST"] = check_rds.DB_CONFIG_ROOT["host"]
        custom_env["DB_PORT"] = str(check_rds.DB_CONFIG_ROOT["port"])
        custom_env["DB_USER"] = check_rds.DB_CONFIG_ROOT["user"]
        custom_env["DB_PASSWD"] = check_rds.DB_CONFIG_ROOT["password"]

        result = subprocess.run(
            [sys.executable, filepath],
            env=custom_env,
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
        )
        logger.info(f"成功: {filepath}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Python 文件执行失败: {filepath}, 错误: {e.stderr}")
        raise Exception(f"运行 Python 文件失败: {filepath}")


def _compare_schema(check_config: CheckConfig, base_rds: CheckRDS,
                    check_rds: CheckRDS, logger: Logger):
    """对比两个数据库的 schema 差异"""
    logger.info(f"对比数据库 schema 差异: {base_rds.DB_TYPE} -> {check_rds.DB_TYPE}")

    for db_name in check_config.DATABASES:
        base_tables = base_rds.list_tables_by_db(db_name)
        check_tables = check_rds.list_tables_by_db(db_name)

        diff = set(check_tables) - set(base_tables)
        if diff:
            raise Exception(f"数据库 {db_name} 表数量不一致, 多出: {diff}")

        if not check_config.AllowTableCompareDismatch:
            diff = set(base_tables) - set(check_tables)
            if diff:
                raise Exception(f"数据库 {db_name} 表数量不一致, 缺少: {diff}")

        for table in base_tables:
            if table not in check_tables:
                logger.warning(f"表 {table} 在对比库中不存在")
                continue

            base_cols = base_rds.get_table_columns(db_name, table)
            check_cols = check_rds.get_table_columns(db_name, table)
            if len(base_cols) != len(check_cols):
                raise Exception(f"表 {db_name}.{table} 列数量不一致")

    logger.info("schema 差异对比完成")
