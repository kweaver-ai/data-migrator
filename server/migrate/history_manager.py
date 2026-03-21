#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""t_schema_migration_history 管理 - 记录 + checksum 比对"""
import datetime
from logging import Logger

from server.db.operate import OperateDB
from server.config.models import RDSConfig
from server.utils.checksum import sha256_file

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
    f_checksum VARCHAR(128) NOT NULL DEFAULT '',
    f_status VARCHAR(32) NOT NULL DEFAULT 'success',
    f_create_time DATETIME NOT NULL
)"""

    def record(self, service_name: str, version: str, script_file_name: str,
               script_path: str, status: str = "success"):
        """记录一条迁移历史"""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        checksum = sha256_file(script_path) if script_path else ""
        self.db.insert(f"{self.deploy_db}.{self.TABLE}", {
            "f_service_name": service_name,
            "f_version": version,
            "f_script_file_name": script_file_name,
            "f_checksum": checksum,
            "f_status": status,
            "f_create_time": now,
        })

    def get_last_record(self, service_name: str) -> dict:
        """查询服务最后一条历史记录"""
        sql = (
            f"SELECT * FROM {self.deploy_db}.{self.TABLE} "
            f"WHERE f_service_name = %s ORDER BY f_id DESC LIMIT 1"
        )
        return self.db.fetch_one(sql, service_name)

    def check_checksum(self, service_name: str, version: str,
                       script_file_name: str, script_path: str) -> bool:
        """比对 checksum，判断脚本是否被篡改"""
        sql = (
            f"SELECT f_checksum FROM {self.deploy_db}.{self.TABLE} "
            f"WHERE f_service_name = %s AND f_version = %s AND f_script_file_name = %s "
            f"ORDER BY f_id DESC LIMIT 1"
        )
        row = self.db.fetch_one(sql, service_name, version, script_file_name)
        if row is None:
            return True  # 无历史记录，无需比对
        stored_checksum = row.get("f_checksum", "")
        if not stored_checksum:
            return True
        current_checksum = sha256_file(script_path)
        if stored_checksum != current_checksum:
            self.logger.warning(
                f"Checksum 不匹配: {service_name}/{version}/{script_file_name}, "
                f"stored={stored_checksum}, current={current_checksum}"
            )
            return False
        return True
