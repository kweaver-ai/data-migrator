#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""t_schema_migration_history 管理 - 历史记录写入"""
import datetime
from logging import Logger

from server.db.operate import OperateDB
from server.config.models import RDSConfig
from server.migrate.task_manager import TaskStatus

class HistoryManager:
    TABLE = "t_schema_migration_history"

    def __init__(self, rds_config: RDSConfig, logger: Logger):
        self.db = OperateDB(rds_config, logger)
        self.rds_config = rds_config
        self.logger = logger
        self.deploy_db = rds_config.get_deploy_db_name()

    @classmethod
    def get_create_table_sql(cls, deploy_db: str) -> str:
        return f"""CREATE TABLE IF NOT EXISTS {deploy_db}.{cls.TABLE} (
    f_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    f_service_name VARCHAR(255) NOT NULL,
    f_version VARCHAR(64) NOT NULL DEFAULT '',
    f_script_file_name VARCHAR(512) NOT NULL DEFAULT '',
    f_status VARCHAR(32) NOT NULL DEFAULT 'success',
    f_create_time DATETIME NOT NULL
)"""

    def record(self, service_name: str, version: str, script_file_name: str,
               status: TaskStatus):
        """记录一条迁移历史"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.insert(f"{self.deploy_db}.{self.TABLE}", {
            "f_service_name": service_name,
            "f_version": version,
            "f_script_file_name": script_file_name,
            "f_status": status,
            "f_create_time": now,
        })
