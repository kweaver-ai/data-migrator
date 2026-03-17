#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""方言工厂：db_type -> 方言实例"""
from logging import Logger

from server.config.models import RDSConfig
from server.db.dialect.base import RDSDialect
from server.db.dialect.mariadb import MariaDBDialect
from server.db.dialect.mysql import MysqlDialect
from server.db.dialect.tidb import TiDBDialect
from server.db.dialect.dm8 import DM8Dialect
from server.db.dialect.kdb9 import KDB9Dialect
from server.db.dialect.goldendb import GoldenDBDialect


_DIALECT_MAP = {
    "mariadb": MariaDBDialect,
    "mysql": MysqlDialect,
    "tidb": TiDBDialect,
    "dm8": DM8Dialect,
    "kdb9": KDB9Dialect,
    "goldendb": GoldenDBDialect,
}


def create_dialect(rds_config: RDSConfig, logger: Logger) -> RDSDialect:
    """根据 db_type 创建方言实例"""
    db_type = rds_config.type.lower()
    dialect_cls = _DIALECT_MAP.get(db_type)
    if dialect_cls is None:
        raise Exception(f"不支持的数据库类型: {db_type}")
    return dialect_cls(rds_config, logger)
