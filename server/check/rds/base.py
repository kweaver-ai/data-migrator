#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""RDS 校验辅助 - 连接配置加载 + 校验"""
import os
from pathlib import Path

import yaml

# 默认配置文件路径（可通过环境变量 CHECK_RDS_CONFIG 覆盖）
_DEFAULT_CONFIG_PATH = Path(__file__).parent / "check_rds_config.yaml"


def load_rds_config() -> dict:
    """加载 RDS 校验连接配置"""
    config_path = os.environ.get("CHECK_RDS_CONFIG", str(_DEFAULT_CONFIG_PATH))
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_rds_config(rds_cfg: dict, required_db_types: list):
    """校验 check_rds_config.yaml 是否包含所有需要的数据库类型"""
    missing = [t for t in required_db_types if t not in rds_cfg]
    if missing:
        config_path = os.environ.get("CHECK_RDS_CONFIG", str(_DEFAULT_CONFIG_PATH))
        raise Exception(
            f"check_rds_config.yaml 缺少以下数据库类型的连接配置: {missing}，"
            f"配置文件路径: {config_path}"
        )
