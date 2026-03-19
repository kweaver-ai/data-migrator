#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Git 拉取 + 目录收集 - 使用 GitPython 从 GitHub 克隆，适配 AppConfig"""
import os
import shutil
from logging import Logger

import git

from server.config.models import AppConfig
from server.utils.version import is_version_dir

DEFAULT_DB_TYPE = "mariadb"


class CollectExecutor:
    def __init__(self, app_config: AppConfig, logger: Logger):
        self.app_config = app_config
        self.logger = logger

    def run(self):
        """收集子命令主入口"""
        self._pull_repos()
        self._collect_repos()

    def _pull_repos(self):
        """从 GitHub 仓库拉取代码（GitPython + sparse-checkout）"""
        self.logger.info("开始拉取代码库")

        source_path = os.path.join(os.getcwd(), "source_code")
        os.makedirs(source_path, exist_ok=True)

        for service_name, service_cfg in self.app_config.services.items():
            project = service_cfg.project
            repo = service_cfg.repo
            ref = service_cfg.ref
            sparse_path = service_cfg.path

            self.logger.info(f"拉取代码库: {project}/{repo}, 分支: {ref}")

            service_path = os.path.join(source_path, service_name)
            if os.path.exists(service_path):
                self.logger.info(f"本地目录已存在: {service_path}")
                continue

            pat = os.getenv("MY_PAT", "")
            if not pat:
                raise Exception("环境变量 MY_PAT 未设置，无法认证 GitHub")

            git_url = f"https://{pat}@github.com/{project}/{repo}.git"
            tmp_path = os.path.join(source_path, f"{service_name}_tmp")

            try:
                # 浅克隆，不自动 checkout
                cloned_repo = git.Repo.clone_from(
                    git_url,
                    tmp_path,
                    depth=1,
                    single_branch=True,
                    branch=ref,
                    no_checkout=True,
                )

                # 配置 sparse-checkout，只检出指定目录
                cloned_repo.git.sparse_checkout("init", "--cone")
                cloned_repo.git.sparse_checkout("set", sparse_path)
                cloned_repo.git.checkout(ref)

                # 将目标目录移动到最终位置
                src_dir = os.path.join(tmp_path, sparse_path)
                os.makedirs(service_path, exist_ok=True)
                dest_dir = os.path.join(service_path, sparse_path)
                shutil.move(src_dir, dest_dir)

                self.logger.info(f"拉取成功: {project}/{repo} -> {service_path}")
            except git.GitCommandError as e:
                raise Exception(
                    f"拉取代码仓库失败: {project}/{repo}, 错误: {e}"
                )
            finally:
                # 清理临时目录（含 .git）
                if os.path.exists(tmp_path):
                    shutil.rmtree(tmp_path)

        self.logger.info("拉取代码库完成")

    def _collect_repos(self):
        """复制数据库类型目录到 repos/"""
        self.logger.info("开始复制代码库目录")

        db_types = self.app_config.db_types
        for service_name, service_cfg in self.app_config.services.items():
            db_path = service_cfg.path

            self.logger.info(f"复制代码库目录: {service_name}, path={db_path}")

            source_path = os.path.join(os.getcwd(), "source_code", service_name, db_path)
            repo_path = os.path.join(self.app_config.repo_path, service_name)
            os.makedirs(repo_path, exist_ok=True)

            for db_type in db_types:
                source_db_path = os.path.join(source_path, db_type)
                if not os.path.isdir(source_db_path):
                    self.logger.info(f"db_type({db_type})不存在，使用默认({DEFAULT_DB_TYPE})")
                    source_db_path = os.path.join(source_path, DEFAULT_DB_TYPE)
                    if not os.path.isdir(source_db_path):
                        raise Exception(f"服务 {service_name} 缺少目录: {db_type}")

                repo_db_path = os.path.join(repo_path, db_type)
                os.makedirs(repo_db_path, exist_ok=True)

                self._copy_version_dirs(source_db_path, repo_db_path)

        self.logger.info("复制代码库目录完成")

    @staticmethod
    def _copy_version_dirs(source_dir: str, dest_dir: str):
        """只复制版本号目录"""
        for name in os.listdir(source_dir):
            if not is_version_dir(name):
                continue
            src = os.path.join(source_dir, name)
            dst = os.path.join(dest_dir, name)
            shutil.copytree(src, dst, dirs_exist_ok=True)
