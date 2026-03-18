#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""目录结构校验 - 复用 tools/check_repo.py，适配 AppConfig

新目录结构：脚本直接放在 <version>/ 下，无子目录。
"""
import os
from logging import Logger

import sqlparse

from server.config.models import AppConfig
from server.check.check_config import CheckConfig
from server.utils.version import VersionUtil


class RepoChecker:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger
        self.check_config = CheckConfig(app_config)

    def run(self):
        """目录结构校验主入口"""
        self.logger.info("开始检查代码库目录结构")

        has_error = False
        for service_name, service_cfg in self.app_config.services.items():
            self.logger.info(f"检查服务目录: {service_name}")
            repo_path = os.path.join(os.getcwd(), "repos", service_name)
            check_from = service_cfg.check_from
            if not self._check_repo(repo_path, check_from):
                has_error = True

        if has_error:
            raise Exception("代码库目录检查失败")

        self.logger.info("代码库目录检查成功")

    def _check_repo(self, repo_path: str, check_from: str) -> bool:
        self.logger.info(f"repo目录: {repo_path}")
        for db_type in self.check_config.DBTypes:
            repo_db_path = os.path.join(repo_path, db_type)
            try:
                self._check_db_type(repo_db_path, check_from)
            except Exception as e:
                self.logger.error(f"check_db_type 失败: {repo_db_path}, 错误: {e}")
                return False
        return True

    def _check_db_type(self, repo_db_path: str, check_from: str):
        self.logger.info(f"db目录: {repo_db_path}")
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

        if self.check_config.CheckType == CheckConfig.CheckLatest:
            if len(versions) >= 1:
                versions = versions[-1:]
        elif self.check_config.CheckType == CheckConfig.CheckRecently:
            if len(versions) >= 2:
                versions = versions[-2:]

        for version in versions:
            version_dir = os.path.join(repo_db_path, version.VersionStr)
            self._check_version(version_dir)

    def _check_version(self, version_dir: str):
        """检查版本目录"""
        self.logger.info(f"version目录: {version_dir}")
        filenames = os.listdir(version_dir)
        if not filenames:
            raise Exception(f"空目录: {version_dir}")

        for filename in filenames:
            filepath = os.path.join(version_dir, filename)
            if os.path.isdir(filepath):
                raise Exception(f"版本目录下不应有子目录: {filepath}")

            if filename == "init.sql":
                self._check_init_file(filepath)
            elif filename.endswith(".json"):
                self.logger.warning(f"跳过 .json 文件: {filepath}")
            elif filename.endswith(".sql") or filename.endswith(".py"):
                self._check_upgrade_file(filepath)
            else:
                raise Exception(f"无效的文件: {filepath}")

    def _check_init_file(self, init_file: str):
        self.logger.info(f"检查init文件: {init_file}")
        with open(init_file, "r", encoding="utf-8") as f:
            sqls_str = f.read()
        formatted = sqlparse.format(sqls_str, strip_comments=True, keyword_case="upper")
        sql_list = sqlparse.split(formatted)
        sql_list = [sql for sql in sql_list if sql.strip() and sql.strip() != ";"]
        if not sql_list:
            self.logger.warning(f"init.sql 为空: {init_file}")

    def _check_upgrade_file(self, filepath: str):
        """检查升级文件命名和格式"""
        filename = os.path.basename(filepath)
        if not (len(filename) >= 3 and filename[:2].isdigit() and filename[2] == "-"):
            raise Exception(f"升级文件命名格式错误 (应为 NN-xxx.sql/py): {filename}")

        if filepath.endswith(".sql"):
            with open(filepath, "r", encoding="utf-8") as f:
                sqls_str = f.read()
            formatted = sqlparse.format(sqls_str, strip_comments=True)
            sql_list = sqlparse.split(formatted)
            if not sql_list:
                self.logger.warning(f"空 SQL 文件: {filepath}")
    # .py 文件只检查命名，不执行
