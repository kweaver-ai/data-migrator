#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/10/18 13:49
@Author  : mario.jiang
@File    : verification_schema_records_table.py
"""
from logging import Logger
from typing import List

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.operate_db import OperateDB


class VerificationSchemaRecords:

    def __init__(self, rds_info: RDSInfo, logger: Logger):
        self.db_info = OperateDB(rds_info, logger)
        self.logger = logger
        self.deploy_db_name = rds_info.get_deploy_db_name()

    def insert(self, ai_id: int, test_entries: List[dict], verify_result: str, verify_end_time: int) -> int:
        try:
            self.cursor = self.db_info.conn.cursor()
            insert_record_sql = f"INSERT INTO {self.deploy_db_name}.verification_data_records" \
                                f" (ai_id, verify_result, verify_end_time)  VALUES " \
                                f"('{ai_id}', '{verify_result}', {verify_end_time});"

            self.cursor.execute(insert_record_sql)
            self.db_info.conn.commit()
            select_record_id_sql = f"SELECT d_id FROM {self.deploy_db_name}.verification_data_records WHERE ai_id={ai_id} AND verify_result='{verify_result}' AND verify_end_time={verify_end_time}"
            self.cursor.execute(select_record_id_sql)
            result_dict = self.cursor.fetchone()
            d_id = result_dict["d_id"]
            for test_record in test_entries:
                insert_entries_sql = f"INSERT INTO {self.deploy_db_name}.data_test_entries (d_id, test_result, test_result_details, service_name) " \
                                     f"VALUES ('{d_id}', '{test_record['test_result']}', '{test_record['test_result_details']}', '{test_record['service_name']}')"
                if test_record['test_result'] == "fail":
                    self.cursor.execute(insert_entries_sql)
            self.db_info.conn.commit()
            return d_id
        except Exception as e:
            raise e
        finally:
            self.cursor.close()
# CREATE TABLE IF NOT EXISTS deploy.verification_data_records (
# 	d_id bigint(11) NOT NULL AUTO_INCREMENT,
# 	ai_id bigint(11) NOT NULL,
# 	verify_result varchar(20) NOT NULL,
# 	verify_end_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
# 	PRIMARY KEY (d_id)
# );





# CREATE TABLE IF NOT EXISTS deploy.data_test_entries (
# 	t_id bigint(11) NOT NULL AUTO_INCREMENT,
# 	d_id bigint(11) NOT NULL,
# 	test_result varchar(20) NOT NULL,
# 	test_result_details varchar(4096) NOT NULL DEFAULT '',
# 	service_name varchar(50) NOT NULL,
# 	PRIMARY KEY (t_id),
# 	KEY d_id (d_id) USING BTREE
# );


