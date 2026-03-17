#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""schema_migration_task 管理 - INSERT/UPDATE/SELECT"""
import datetime
from logging import Logger

from server.db.operate import OperateDB
from server.config.models import RDSConfig

TABLE = "schema_migration_task"


class TaskManager:
    def __init__(self, rds_config: RDSConfig, logger: Logger):
        self.db = OperateDB(rds_config, logger)
        self.rds_config = rds_config
        self.logger = logger
        self.deploy_db = rds_config.get_deploy_db_name()

    def select_task(self, service_name: str) -> dict:
        """查询服务的迁移任务记录"""
        sql = (
            f"SELECT * FROM {self.deploy_db}.{TABLE} "
            f"WHERE service_name = %s ORDER BY id DESC LIMIT 1"
        )
        return self.db.fetch_one(sql, service_name)

    def insert_task(self, service_name: str, installed_version: str,
                    target_version: str, script_file_name: str, status: str = "running"):
        """插入新任务记录"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.db.insert(f"{self.deploy_db}.{TABLE}", {
            "service_name": service_name,
            "installed_version": installed_version,
            "target_version": target_version,
            "script_file_name": script_file_name,
            "status": status,
            "create_time": now,
            "update_time": now,
        })

    def update_status(self, service_name: str, status: str,
                      script_file_name: str = None,
                      target_version: str = None,
                      installed_version: str = None):
        """更新任务状态"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sets = ["status = %s", "update_time = %s"]
        params = [status, now]

        if script_file_name is not None:
            sets.append("script_file_name = %s")
            params.append(script_file_name)
        if target_version is not None:
            sets.append("target_version = %s")
            params.append(target_version)
        if installed_version is not None:
            sets.append("installed_version = %s")
            params.append(installed_version)

        params.append(service_name)
        sql = (
            f"UPDATE {self.deploy_db}.{TABLE} "
            f"SET {', '.join(sets)} "
            f"WHERE service_name = %s"
        )
        self.db.execute(sql, *params)

    def update_service_name(self, old_name: str, new_name: str):
        """服务改名"""
        sql = (
            f"UPDATE {self.deploy_db}.{TABLE} "
            f"SET service_name = %s WHERE service_name = %s"
        )
        self.db.execute(sql, new_name, old_name)
