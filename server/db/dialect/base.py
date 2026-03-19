#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据库方言抽象基类 - 去掉用户管理方法，保留 SQL 模板和 init_db_config"""
from abc import ABC, abstractmethod
from logging import Logger

from server.config.models import RDSConfig
from server.db.connection import DatabaseConnection


class RDSDialect(ABC):
    """抽象基类，定义各数据库类型的 SQL 模板和特殊初始化"""

    def __init__(self, rds_config: RDSConfig, logger: Logger):
        self.logger = logger
        self.rds_config = rds_config
        self.DB_TYPE = rds_config.type.lower()
        self._database_connection = DatabaseConnection(rds_config)
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            self._conn = self._database_connection.get_conn()
        return self._conn

    @abstractmethod
    def init_db_config(self):
        """数据库类型特殊初始化配置"""
        pass

    # SQL 模板属性（子类覆盖）
    SET_DATABASE_SQL = ""
    QUERY_TABLE_SQL = ""
    QUERY_COLUMN_SQL = ""
    QUERY_INDEX_SQL = None
    QUERY_CONSTRAINT_SQL = None
    CREATE_DATABASE_SQL = ""
    DROP_DATABASE_SQL = ""

    # JSON 升级文件执行模板（子类覆盖，None 表示不支持）
    ADD_COLUMN_SQL = None
    MODIFY_COLUMN_SQL = None
    RENAME_COLUMN_SQL = None
    DROP_COLUMN_SQL = None

    ADD_INDEX_SQL = None
    RENAME_INDEX_SQL = None
    DROP_INDEX_SQL = None

    ADD_CONSTRAINT_SQL = None
    RENAME_CONSTRAINT_SQL = None
    DROP_CONSTRAINT_SQL = None

    RENAME_TABLE_SQL = None
    DROP_TABLE_SQL = None
