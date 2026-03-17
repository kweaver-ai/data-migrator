#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.db.operate_db import OperateDB


class InitDMTable:

    def __init__(self, cfg: ConfigData):
        self.cfg = cfg
        self.logger = self.cfg.logger
        self.operate = OperateDB(self.cfg.rds, self.logger)

    def select_table_from_deploy_databases_dm(self, deploy_db_name: str):
        sql = "select TABLE_NAME from all_tables where OWNER= %s"
        return self.operate.fetch_all_result(sql, deploy_db_name)

    def select_table_from_deploy_databases_kdb9(self, deploy_db_name: str):
        sql = "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA= %s"
        return self.operate.fetch_all_result(sql, deploy_db_name)

    def select_table_from_deploy_databases(self, deploy_db_name: str):
        sql = f"show tables from {deploy_db_name}"
        return self.operate.fetch_all_result(sql)
