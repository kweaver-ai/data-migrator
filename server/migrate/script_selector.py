#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""脚本选择器 - 版本扫描、init.sql 查找、脚本排序

新目录结构：脚本直接放在 <version>/ 下，无 pre/ 子目录。
"""
import os
import re
from logging import Logger
from typing import List, Optional, Tuple

from server.config.models import AppConfig
from server.utils.version import (
    compare_version, get_max_version, sort_versions, extract_number, is_version_dir
)


class ScriptSelector:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger

    def get_service_db_type_path(self, service_name: str) -> str:
        """获取 <script_dir>/<service>/<db_type>/ 路径"""
        return os.path.join(
            self.app_config.script_directory_path,
            service_name,
            self.app_config.rds.type.lower(),
        )

    def get_all_versions(self, service_name: str) -> List[str]:
        """获取服务下所有版本目录"""
        db_type_path = self.get_service_db_type_path(service_name)
        if not os.path.isdir(db_type_path):
            return []
        return [d for d in os.listdir(db_type_path)
                if os.path.isdir(os.path.join(db_type_path, d)) and is_version_dir(d)]

    def get_max_version(self, service_name: str) -> Optional[str]:
        """获取服务的最大版本号"""
        versions = self.get_all_versions(service_name)
        return get_max_version(versions)

    def find_init_sql(self, service_name: str) -> Optional[str]:
        """
        逆序查找 init.sql：从最大版本开始向下找，返回第一个含 init.sql 的版本路径。
        """
        versions = sort_versions(self.get_all_versions(service_name))
        db_type_path = self.get_service_db_type_path(service_name)

        for version in reversed(versions):
            init_path = os.path.join(db_type_path, version, "init.sql")
            if os.path.isfile(init_path):
                self.logger.info(f"[{service_name}] 找到 init.sql: {init_path}")
                return init_path
        return None

    def select_upgrade_scripts(self, service_name: str,
                               installed_version: Optional[str]) -> Tuple[List[List[str]], str, bool]:
        """
        选择待执行的升级脚本。

        首次安装（installed_version 为 None）：
          - 逆序找 init.sql(V_base) + V_base 之后的增量脚本

        升级（installed_version 不为 None）：
          - 跳过 init.sql
          - 从 installed_version 之后的版本开始，收集增量脚本

        返回: (upgrade_files_list, max_version, has_scripts)
          - upgrade_files_list: [[version1 scripts], [version2 scripts], ...]
          - max_version: 最大版本号
          - has_scripts: 是否有脚本需要执行
        """
        versions = sort_versions(self.get_all_versions(service_name))
        if not versions:
            return [], "", False

        max_version = versions[-1]
        db_type_path = self.get_service_db_type_path(service_name)

        upgrade_files_list = []

        for version in versions:
            # 升级模式：跳过 <= installed_version 的版本
            if installed_version and compare_version(version, installed_version) <= 0:
                continue

            version_path = os.path.join(db_type_path, version)
            scripts = self._collect_scripts_from_dir(version_path)

            if scripts:
                upgrade_files_list.append(scripts)

        has_scripts = len(upgrade_files_list) > 0
        return upgrade_files_list, max_version, has_scripts

    def _collect_scripts_from_dir(self, version_path: str) -> List[str]:
        """
        收集版本目录下的升级脚本（NN-xxx.sql/py，跳过 init.sql）。
        .json 文件输出警告并跳过。
        """
        if not os.path.isdir(version_path):
            return []

        scripts = []
        for filename in os.listdir(version_path):
            filepath = os.path.join(version_path, filename)
            if not os.path.isfile(filepath):
                continue

            # 跳过 init.sql
            if filename == "init.sql":
                continue

            # .json 文件：警告并跳过
            if filename.endswith(".json"):
                self.logger.warning(f"跳过 .json 文件: {filepath}")
                continue

            # 匹配 NN-xxx.sql 或 NN-xxx.py
            if re.match(r"^\d+-.*\.(sql|py)$", filename):
                scripts.append(filepath)

        if scripts:
            scripts.sort(key=extract_number)
        return scripts
