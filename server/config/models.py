#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""配置数据类"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


VALID_SOURCE_TYPES = {"internal", "external"}


@dataclass
class RDSConfig:
    """数据库连接配置"""
    host: str
    port: int
    user: str
    password: str
    type: str  # mariadb, mysql, tidb, dm8, kdb9, goldendb
    source_type: str  # internal, external

    def __post_init__(self):
        if self.source_type not in VALID_SOURCE_TYPES:
            raise ValueError(
                f"source_type 非法值: '{self.source_type}'，只允许: {sorted(VALID_SOURCE_TYPES)}"
            )

    def get_deploy_db_name(self) -> str:
        return "deploy"


@dataclass
class ServiceConfig:
    """单个服务的配置"""
    project: str = ""
    repo: str = ""
    ref: str = ""
    path: str = ""
    check_from: Optional[str] = None


@dataclass
class CheckRulesConfig:
    """校验规则配置"""
    check_type: int = 1  # 1=Latest, 2=Recently, 3=All
    allow_none_primary_key: bool = False
    allow_foreign_key: bool = False
    allow_python_exception: bool = False
    allow_table_compare_dismatch: bool = False


@dataclass
class AppConfig:
    """应用总配置"""
    rds: RDSConfig
    services: Dict[str, ServiceConfig] = field(default_factory=dict)
    db_types: List[str] = field(default_factory=lambda: ["mariadb"])
    databases: List[str] = field(default_factory=list)
    check_rules: CheckRulesConfig = field(default_factory=CheckRulesConfig)
    repo_path: str = ""
    renamed_services: List[dict] = field(default_factory=list)
    service_filter: Optional[List[str]] = None
