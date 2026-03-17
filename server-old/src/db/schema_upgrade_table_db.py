#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/8/9 15:59
@Author  : mario.jiang
@File    : schema_upgrade_table.py
"""
from logging import Logger


from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.operate_db import OperateDB


class SchemaUpgradeTableDB:

    def __init__(self, rds_info: RDSInfo, logger: Logger):
        self.db_info = OperateDB(rds_info, logger)
        self.logger = logger
        self.deploy_db_name = rds_info.get_deploy_db_name()

    def get_micro_service_installed_version(self, micro_service_name):
        try:
            self.cursor = self.db_info.conn.cursor()
            sql = f"select installed_version from {self.deploy_db_name}.schema_upgrade_table where service_name= '{micro_service_name}'"
            self.cursor.execute(sql)
            results = self.cursor.fetchone()
            if results:
                return results["installed_version"]
            else:
                return None
        except Exception as e:
            raise e
        finally:
            self.cursor.close()




