#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/5/3 21:32
@Author  : mario.jiang
@File    : schema_record_db.py
"""
import datetime
from logging import Logger

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.operate_db import OperateDB


class SchemaRecordDB(object):

    def __init__(self, rds_info: RDSInfo, logger: Logger):
        self.db_info = OperateDB(rds_info, logger)
        self.logger = logger
        self.deploy_db_name = rds_info.get_deploy_db_name()

    def insert_schema_change_record(self, service_name, installed_version, script_file_name, target_version, create_time=None):
        if not create_time:
            now = datetime.datetime.now()
            now = now.strftime('%Y-%m-%d %H:%M:%S')
            create_time = now
        col = dict()
        col["service_name"] = service_name
        col["installed_version"] = installed_version
        col["script_file_name"] = script_file_name
        col["target_version"] = target_version
        col["create_time"] = create_time
        table = f"{self.deploy_db_name}.schema_records"
        return self.db_info.insert(table=table, columns=col)
