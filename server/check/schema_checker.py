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


class SchemaChecker:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger
        self.check_config = CheckConfig(app_config)

    def run(self):
        """SQL 执行校验主入口"""
        self.logger.info("开始验证数据模型脚本")

        self._reset_schema()

        for service_name, service_cfg in self.app_config.services.items():
            self.logger.info(f"验证服务: {service_name}")
            repo_path = os.path.join(os.getcwd(), "repos", service_name)
            check_from = service_cfg.check_from
            if not self._check_repo(repo_path, check_from):
                raise Exception(f"数据模型 {service_name} 验证失败")

        self.logger.info("数据模型验证成功")

    def _create_check_rds(self, db_type: str, is_primary: bool = True) -> CheckRDS:
        if db_type == "dm8":
            return CheckDM8(self.check_config, is_primary=is_primary)
        elif db_type == "mariadb":
            return CheckMariaDB(self.check_config, is_primary=is_primary)
        elif db_type == "kdb9":
            return CheckKDB9(self.check_config, is_primary=is_primary)
        else:
            raise Exception(f"不支持的数据库类型: {db_type}")

    def _reset_schema(self):
        self.logger.info("重置数据模式")
        for db_type in self.check_config.DBTypes:
            primary = self._create_check_rds(db_type, is_primary=True)
            secondary = self._create_check_rds(db_type, is_primary=False)
            try:
                primary.reset_schema(self.check_config.DATABASES)
                secondary.reset_schema(self.check_config.DATABASES)
            except Exception as e:
                self.logger.error(f"reset_schema 失败: {db_type}, 错误: {e}")
                raise

    def _check_repo(self, repo_path: str, check_from: str) -> bool:
        base_rds = None
        for db_type in self.check_config.DBTypes:
            primary = self._create_check_rds(db_type, is_primary=True)
            secondary = self._create_check_rds(db_type, is_primary=False)

            repo_db_path = os.path.join(repo_path, db_type)
            try:
                self._check_db_type(repo_db_path, primary, secondary, check_from)
            except Exception as e:
                self.logger.error(f"check_db_type 失败: {repo_db_path}, 错误: {e}")
                return False

            if base_rds is None:
                base_rds = primary
            else:
                self._compare_schema(base_rds, primary)

        return True

    def _check_db_type(self, repo_db_path: str, primary: CheckRDS,
                       secondary: CheckRDS, check_from: str):
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
                self._check_version_init(version_dir, primary)
            # 执行增量脚本
            self._check_version_upgrades(version_dir, primary)

        # 对比最新一次 init
        if len(versions) >= 1:
            last_dir = os.path.join(repo_db_path, versions[-1].VersionStr)
            self._check_version_init(last_dir, secondary)

    def _check_version_init(self, version_dir: str, check_rds: CheckRDS):
        """执行 init.sql"""
        init_file = os.path.join(version_dir, "init.sql")
        if not os.path.isfile(init_file):
            return

        self.logger.info(f"执行 init.sql: {init_file}")
        with open(init_file, "r", encoding="utf-8") as f:
            sqls_str = f.read()
        formatted = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
        sql_list = sqlparse.split(formatted)
        sql_list = [sql for sql in sql_list if sql.strip() and sql.strip() != ";"]
        if sql_list:
            check_rds.run_sql(sql_list)

    def _check_version_upgrades(self, version_dir: str, check_rds: CheckRDS):
        """执行版本目录下的增量脚本"""
        filenames = sorted(os.listdir(version_dir))
        for filename in filenames:
            if filename == "init.sql":
                continue
            filepath = os.path.join(version_dir, filename)
            if not os.path.isfile(filepath):
                continue

            if filename.endswith(".json"):
                self._check_json_file(filepath, check_rds)
                continue

            if filename.endswith(".sql"):
                self._check_sql_file(filepath, check_rds)
            elif filename.endswith(".py"):
                self._check_py_file(filepath, check_rds)

    def _check_sql_file(self, filepath: str, check_rds: CheckRDS):
        self.logger.info(f"执行 SQL 文件: {filepath}")
        with open(filepath, "r", encoding="utf-8") as f:
            sqls_str = f.read()
        formatted = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
        sql_list = sqlparse.split(formatted)
        sql_list = [sql for sql in sql_list if sql.strip() and sql.strip() != ";"]
        if sql_list:
            check_rds.run_sql(sql_list)

    def _check_json_file(self, filepath: str, check_rds: CheckRDS):
        """执行 JSON 升级文件 — 按操作类型调用 CheckRDS 方法"""
        self.logger.info(f"执行 JSON 文件: {filepath}")
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                update_items = json.load(f)
            except json.JSONDecodeError as e:
                raise Exception(f"无效的JSON文件: {filepath}, {e}")
            if not isinstance(update_items, list):
                raise Exception(f"JSON根类型必须为对象(list): {filepath}")

        if len(update_items) <= 0:
            raise Exception(f"JSON列表不能为空: {filepath}")

        required_fields = ["db_name", "table_name", "object_type", "operation_type",
                           "object_name", "object_property", "object_comment"]
        allowed_object_types = {"COLUMN", "INDEX", "UNIQUE INDEX", "CONSTRAINT", "TABLE", "DB"}
        allowed_operation_types = {"ADD", "DROP", "MODIFY", "RENAME"}

        for item in update_items:
            if not isinstance(item, dict):
                raise Exception(f"格式错误: {item}")

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
            elif object_type in ("INDEX", "UNIQUE INDEX"):
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

    def _check_py_file(self, filepath: str, check_rds: CheckRDS):
        self.logger.info(f"执行 Python 文件: {filepath}")
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
            self.logger.info(f"成功: {filepath}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Python 文件执行失败: {filepath}, 错误: {e.stderr}")
            raise Exception(f"运行 Python 文件失败: {filepath}")

    def _compare_schema(self, base_rds: CheckRDS, check_rds: CheckRDS):
        """对比两个数据库的 schema 差异"""
        self.logger.info(f"对比数据库 schema 差异: {base_rds.DB_TYPE} -> {check_rds.DB_TYPE}")

        for db_name in self.check_config.DATABASES:
            base_tables = base_rds.list_tables_by_db(db_name)
            check_tables = check_rds.list_tables_by_db(db_name)

            diff = set(check_tables) - set(base_tables)
            if diff:
                raise Exception(f"数据库 {db_name} 表数量不一致, 多出: {diff}")

            if not self.check_config.AllowTableCompareDismatch:
                diff = set(base_tables) - set(check_tables)
                if diff:
                    raise Exception(f"数据库 {db_name} 表数量不一致, 缺少: {diff}")

            for table in base_tables:
                if table not in check_tables:
                    self.logger.warning(f"表 {table} 在对比库中不存在")
                    continue

                base_cols = base_rds.get_table_columns(db_name, table)
                check_cols = check_rds.get_table_columns(db_name, table)
                if len(base_cols) != len(check_cols):
                    raise Exception(f"表 {db_name}.{table} 列数量不一致")

        self.logger.info("schema 差异对比完成")
