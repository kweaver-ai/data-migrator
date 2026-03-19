#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""YAML 配置加载 + AppConfig 构建"""
import os
from logging import Logger
from typing import List, Optional

import yaml

from server.config.models import AppConfig, RDSConfig, ServiceConfig, CheckRulesConfig


def load_config(config_path: str, service_filter: Optional[List[str]], logger: Logger) -> AppConfig:
    """
    加载 YAML 配置文件并构建 AppConfig。
    service_filter: CLI 传入的 --service 参数，用于过滤服务范围；None 表示全部。
    """
    logger.info(f"加载配置文件: {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # ── RDS ──
    rds_data = cfg.get("rds", cfg.get("depServices", {}).get("rds", {}))
    rds_config = RDSConfig(
        host=rds_data.get("host", ""),
        port=int(rds_data.get("port", 3306)),
        user=rds_data.get("user", ""),
        password=rds_data.get("password", ""),
        type=rds_data.get("type", "mariadb"),
        system_id=rds_data.get("system_id", ""),
    )

    # ── services ──
    raw_services = cfg.get("services", {})
    services = {}
    for name, info in raw_services.items():
        if service_filter and name not in service_filter:
            continue
        services[name] = ServiceConfig(
            project=info.get("project", ""),
            repo=info.get("repo", ""),
            ref=info.get("ref", ""),
            path=info.get("path", ""),
            check_from=info.get("check_from"),
        )

    # ── check_rules ──
    raw_rules = cfg.get("check_rules", {})
    check_rules = CheckRulesConfig(
        check_type=raw_rules.get("check_type", 1),
        allow_none_primary_key=raw_rules.get("allow_none_primary_key", False),
        allow_foreign_key=raw_rules.get("allow_foreign_key", False),
        allow_python_exception=raw_rules.get("allow_python_exception", False),
        allow_table_compare_dismatch=raw_rules.get("allow_table_compare_dismatch", False),
    )

    # ── repo_path ──
    repo_path = os.path.join(os.getcwd(), "repos")

    app_config = AppConfig(
        rds=rds_config,
        services=services,
        db_types=[t.lower() for t in cfg.get("db_types", ["mariadb"])],
        databases=[d.lower() for d in cfg.get("databases", [])],
        check_rules=check_rules,
        repo_path=repo_path,
        renamed_services=cfg.get("renamed_services") or [],
        service_filter=service_filter or None,
    )

    # 注入环境变量供 .py 脚本使用
    os.environ["DB_HOST"] = os.environ.get("DB_HOST") or rds_config.host
    os.environ["DB_PORT"] = os.environ.get("DB_PORT") or str(rds_config.port)
    os.environ["DB_USER"] = os.environ.get("DB_USER") or rds_config.user
    os.environ["DB_PASSWD"] = os.environ.get("DB_PASSWD") or rds_config.password
    os.environ["DB_TYPE"] = rds_config.type
    os.environ["SYSTEM_ID"] = os.environ.get("SYSTEM_ID") or rds_config.system_id

    logger.info(f"配置加载完成, 服务数: {len(services)}, db_type: {rds_config.type}")
    return app_config
