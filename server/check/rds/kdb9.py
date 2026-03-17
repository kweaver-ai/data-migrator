#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""KDB9 校验实现"""
from server.check.rds.base import CheckRDS, load_rds_config
from server.check.check_config import CheckConfig


class CheckKDB9(CheckRDS):
    def __init__(self, check_config: CheckConfig, is_primary: bool = True):
        self.DB_TYPE = "KDB9"

        rds_cfg = load_rds_config()["kdb9"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        self.DB_CONFIG_ROOT = {**section, "DB_TYPE": self.DB_TYPE}

        self.SET_DATABASE_SQL = "SET SEARCH_PATH TO {db_name}"
        self.QUERY_DATABASES_SQL = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE catalog_name='proton'"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"
        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='proton' AND TABLE_SCHEMA='{db_name}'"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='{db_name}' AND TABLE_NAME='{table_name}'"
        self.COLUMN_NAME_FIELD = "COLUMN_NAME"

        super().__init__(check_config)

    def check_init(self, sql_list: list):
        pass

    def check_update(self, sql_list: list):
        pass

    def get_column_type(self, column: dict) -> tuple:
        data_type = column["DATA_TYPE"].upper()
        if data_type == "USER-DEFINED":
            data_type = column.get("COLUMN_TYPE", "").upper()

        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE", "REAL"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("BOOLEAN",):
            return data_type, "BooleanType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "TEXT",
                           "TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT"):
            return data_type, "StringType"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            return data_type, "DateAndTimeType"
        return data_type, "UNKNOWN"
