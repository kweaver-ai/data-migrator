#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.connection import DatabaseConnection


class RDSRootQuery:
    """使用"""

    def __init__(self, rds_info: RDSInfo):
        self.rds_info = rds_info
        database_connection = DatabaseConnection(self.rds_info)
        self._root_conn_db = database_connection.get_root_conn()
        self.deploy_db_name = rds_info.get_deploy_db_name()

    def select_all_databases(self):
        """查询所有的数据库"""

        sql = "show databases"
        cursor = self._root_conn_db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results

    def select_user_form_database(self, user_name):
        """查询指定用户"""
        sql = """SELECT User FROM mysql.user WHERE User = %s """
        cursor = self._root_conn_db.cursor()
        cursor.execute(sql, user_name)
        result = cursor.fetchone()
        cursor.close()
        return result

    def get_user_database_permissions(self, user: str):
        sql = f"SELECT DISTINCT Db FROM mysql.db WHERE User = '{user}' AND Host = '%';"
        cursor = self._root_conn_db.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results



