#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MariaDB 校验实现"""
from server.check.rds.base import CheckRDS, load_rds_config
from server.check.check_config import CheckConfig


class CheckMariaDB(CheckRDS):
    def __init__(self, check_config: CheckConfig, is_primary: bool = True):
        self.DB_TYPE = "MARIADB"

        rds_cfg = load_rds_config()["mariadb"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        self.DB_CONFIG_ROOT = {**section, "DB_TYPE": self.DB_TYPE}

        self.SET_DATABASE_SQL = "USE {db_name}"
        self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"
        self.CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS {db_name} CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        self.DROP_DATABASE_SQL = "DROP DATABASE IF EXISTS {db_name}"
        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{db_name}'"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.COLUMN_NAME_FIELD = "COLUMN_NAME"

        super().__init__(check_config)

    def check_init(self, sql_list: list):
        pass

    def check_update(self, sql_list: list):
        pass

    def get_column_type(self, column: dict) -> tuple:
        data_type = column["DATA_TYPE"].upper()
        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT", "BOOLEAN"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "TINYBLOB", "BLOB",
                           "MEDIUMBLOB", "LONGBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"):
            return data_type, "StringType"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            return data_type, "DateAndTimeType"
        return data_type, "UNKNOWN"
