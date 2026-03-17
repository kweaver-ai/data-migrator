#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import os

import rdsdriver


from dataModelManagement.src.data_classes.rds_info import RDSInfo


class DatabaseConnection(object):

    def __init__(self, rds_info: RDSInfo):
        self.rds_info = rds_info

    def get_root_conn(self):
        """
        获取数据库root用户权限的连接
        :return:
        """
        admin_key = self.rds_info.admin_key
        admin_key = base64.b64decode(admin_key).decode('utf-8')
        db_information = admin_key.split(":")
        admin_user = db_information[0]
        admin_password = db_information[1]

        # 内置数据库场景直接使用root用户
        os.environ["DB_USER"] = admin_user
        os.environ["DB_PASSWD"] = admin_password

        db = rdsdriver.connect(host=self.rds_info.host,
                               user=admin_user,
                               password=admin_password,
                               port=self.rds_info.port,
                               connect_timeout=20,
                               autocommit=True,
                               charset='utf8mb4',
                               cursorclass=rdsdriver.DictCursor,
                               )
        return db

    def get_conn(self):
        """
        获取数据库连接
        :return:
        """
        db = rdsdriver.connect(host=self.rds_info.host,
                               port=self.rds_info.port,
                               user=self.rds_info.user,
                               password=self.rds_info.password,
                               connect_timeout=20,
                               autocommit=True,
                               charset='utf8mb4',
                               cursorclass=rdsdriver.DictCursor,
                               )
        return db
