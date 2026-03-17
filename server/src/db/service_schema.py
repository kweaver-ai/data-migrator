#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/2/16 15:26
@Author  : mario.jiang
@File    : schema.py
"""
from logging import Logger


from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.operate_db import OperateDB


class ServiceSchema:

    def __init__(self, rds_info: RDSInfo, logger: Logger):
        self.db_info = OperateDB(rds_info, logger)
        self.logger = logger
        self.deploy_db_name = rds_info.get_deploy_db_name()

    def get_table_schema(self, target_tables_dict: dict):
        try:
            self.cursor = self.db_info.conn.cursor()
            target_tables = target_tables_dict.keys()
            current_tables = {}

            for target_table in target_tables:
                table_info = target_table.split('.')
                database = table_info[0].replace("`", "")
                table = table_info[1].replace("`", "")
                query_sql = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE" \
                            f" TABLE_SCHEMA = '{database}' AND TABLE_NAME = '{table}';"
                # query_sql = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE" \
                #             f" TABLE_SCHEMA = %s AND TABLE_NAME = %s;"
                self.cursor.execute(query_sql)
                result = self.cursor.fetchone()
                if result:
                    # self.cursor.execute(f"show create table {target_table}")
                    self.cursor.execute(f"show create table {target_table}")
                    init_table_schema = self.cursor.fetchone()
                    current_tables[target_table] = init_table_schema
            return current_tables
        except Exception as e:
            raise e
        finally:
            self.cursor.close()
