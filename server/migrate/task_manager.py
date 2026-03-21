#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""t_schema_migration_task 管理 - INSERT/UPDATE/SELECT"""
import datetime
from enum import Enum
from logging import Logger

from server.db.operate import OperateDB
from server.config.models import RDSConfig


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED  = "failed"


class TaskManager:
    TABLE = "t_schema_migration_task"

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
    f_installed_version VARCHAR(64) NOT NULL DEFAULT '',
    f_target_version VARCHAR(64) NOT NULL DEFAULT '',
    f_script_file_name VARCHAR(512) NOT NULL DEFAULT '',
    f_status VARCHAR(32) NOT NULL DEFAULT 'pending',
    f_create_time DATETIME NOT NULL,
    f_update_time DATETIME NOT NULL
)"""

    def select_task(self, service_name: str) -> dict:
        """查询服务的迁移任务记录"""
        sql = (
            f"SELECT * FROM {self.deploy_db}.{self.TABLE} "
            f"WHERE f_service_name = %s ORDER BY f_id DESC LIMIT 1"
        )
        return self.db.fetch_one(sql, service_name)

    def insert_task(self, service_name: str, installed_version: str,
                    target_version: str, script_file_name: str, status: TaskStatus):
        """插入新任务记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.insert(f"{self.deploy_db}.{self.TABLE}", {
            "f_service_name": service_name,
            "f_installed_version": installed_version,
            "f_target_version": target_version,
            "f_script_file_name": script_file_name,
            "f_status": status,
            "f_create_time": now,
            "f_update_time": now,
        })

    def update_status(self, service_name: str, status: TaskStatus,
                      script_file_name: str = None,
                      target_version: str = None,
                      installed_version: str = None):
        """更新任务状态"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sets = ["f_status = %s", "f_update_time = %s"]
        params = [status, now]

        if script_file_name is not None:
            sets.append("f_script_file_name = %s")
            params.append(script_file_name)
        if target_version is not None:
            sets.append("f_target_version = %s")
            params.append(target_version)
        if installed_version is not None:
            sets.append("f_installed_version = %s")
            params.append(installed_version)

        params.append(service_name)
        sql = (
            f"UPDATE {self.deploy_db}.{self.TABLE} "
            f"SET {', '.join(sets)} "
            f"WHERE f_service_name = %s"
        )
        self.db.execute(sql, *params)

    def update_service_name(self, old_name: str, new_name: str):
        """服务改名"""
        sql = (
            f"UPDATE {self.deploy_db}.{self.TABLE} "
            f"SET f_service_name = %s WHERE f_service_name = %s"
        )
        self.db.execute(sql, new_name, old_name)
