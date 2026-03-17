#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Git 拉取 + 目录收集 - 复用 tools/collect_repo.py 逻辑，适配 AppConfig"""
import os
import shutil
import subprocess
from logging import Logger

from server.config.models import AppConfig
from server.utils.version import is_version_dir

DEFAULT_DB_TYPE = "mariadb"
COMMON_URL = "https://devops.aishu.cn/AISHUDevOps/{}/_git/{}"


def run_collect(app_config: AppConfig, logger: Logger):
    """收集子命令主入口"""
    pull_repos(app_config, logger)
    collect_repos(app_config, logger)


def pull_repos(app_config: AppConfig, logger: Logger):
    """从 Git 仓库拉取代码"""
    logger.info("开始拉取代码库")

    source_path = os.path.join(os.getcwd(), "source_code")
    os.makedirs(source_path, exist_ok=True)

    pat = os.getenv("PAT", "")
    base_commands = [
        "set -ex",
        f"MY_PAT={pat}",
        """B64_PAT=$(printf ":$MY_PAT" | base64)""",
    ]

    for service_name, service_cfg in app_config.services.items():
        project = service_cfg.project
        repo = service_cfg.repo
        ref = service_cfg.ref

        logger.info(f"拉取代码库: {project}/{repo}, 分支: {ref}")

        service_path = os.path.join(source_path, service_name)
        if os.path.exists(service_path):
            logger.info(f"本地目录已存在: {service_path}")
            continue

        git_url = COMMON_URL.format(project, repo)
        commands = base_commands + [
            "set -ex",
            f"ref={ref}",
            f"git_url={git_url}",
            f"service_path={service_path}",
            'git clone -c http.extraHeader="Authorization: Basic ${B64_PAT}" '
            "--depth 1 -b ${ref} ${git_url} ${service_path}",
        ]
        commands_str = " && ".join(commands)

        try:
            custom_env = os.environ.copy()
            result = subprocess.run(
                commands_str,
                env=custom_env,
                capture_output=True,
                shell=True,
                text=True,
                check=True,
            )
            logger.info(f"拉取成功: {result.stdout}")
        except subprocess.CalledProcessError as e:
            raise Exception(
                f"拉取代码仓库失败: {project}/{repo}, "
                f"返回码: {e.returncode}, 错误: {e.stderr}"
            )

    logger.info("拉取代码库完成")


def collect_repos(app_config: AppConfig, logger: Logger):
    """复制数据库类型目录到 repos/"""
    logger.info("开始复制代码库目录")

    db_types = app_config.db_types
    for service_name, service_cfg in app_config.services.items():
        db_path = service_cfg.path

        logger.info(f"复制代码库目录: {service_name}, path={db_path}")

        source_path = os.path.join(os.getcwd(), "source_code", service_name, db_path)
        repo_path = os.path.join(os.getcwd(), "repos", service_name)
        os.makedirs(repo_path, exist_ok=True)

        for db_type in db_types:
            source_db_path = os.path.join(source_path, db_type)
            if not os.path.isdir(source_db_path):
                logger.info(f"db_type({db_type})不存在，使用默认({DEFAULT_DB_TYPE})")
                source_db_path = os.path.join(source_path, DEFAULT_DB_TYPE)
                if not os.path.isdir(source_db_path):
                    raise Exception(f"服务 {service_name} 缺少目录: {db_type}")

            repo_db_path = os.path.join(repo_path, db_type)
            os.makedirs(repo_db_path, exist_ok=True)

            _copy_version_dirs(source_db_path, repo_db_path)

    logger.info("复制代码库目录完成")


def _copy_version_dirs(source_dir: str, dest_dir: str):
    """只复制版本号目录"""
    for name in os.listdir(source_dir):
        if not is_version_dir(name):
            continue
        src = os.path.join(source_dir, name)
        dst = os.path.join(dest_dir, name)
        shutil.copytree(src, dst, dirs_exist_ok=True)
