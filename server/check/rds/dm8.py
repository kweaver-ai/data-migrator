#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DM8 校验实现"""
from server.check.rds.base import CheckRDS, load_rds_config
from server.check.check_config import CheckConfig


class CheckDM8(CheckRDS):
    def __init__(self, check_config: CheckConfig, is_primary: bool = True):
        self.DB_TYPE = "DM8"

        rds_cfg = load_rds_config()["dm8"]
        section = rds_cfg["primary"] if is_primary else rds_cfg["secondary"]
        self.DB_CONFIG_ROOT = {**section, "DB_TYPE": self.DB_TYPE}

        self.SET_DATABASE_SQL = "SET SCHEMA {db_name}"
        self.QUERY_DATABASES_SQL = "select OWNER from dba_objects where object_type='SCH'"
        self.CREATE_DATABASE_SQL = "CREATE SCHEMA {db_name}"
        self.DROP_DATABASE_SQL = "DROP SCHEMA {db_name} CASCADE"
        self.QUERY_TABLES_SQL = "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER='{db_name}'"
        self.QUERY_COLUMNS_SQL = "SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER='{db_name}' AND TABLE_NAME='{table_name}'"
        self.COLUMN_NAME_FIELD = "COLUMN_NAME"

        super().__init__(check_config)

    def check_init(self, sql_list: list):
        pass

    def check_update(self, sql_list: list):
        pass

    def get_column_type(self, column: dict) -> tuple:
        data_type = column["DATA_TYPE"].upper()
        if data_type in ("INTEGER", "INT", "SMALLINT", "TINYINT", "BYTE", "MEDIUMINT", "BIGINT"):
            return data_type, "IntegerType"
        elif data_type in ("DECIMAL", "NUMERIC"):
            return data_type, "FixedPointType"
        elif data_type in ("FLOAT", "DOUBLE", "REAL"):
            return data_type, "FloatingPointType"
        elif data_type in ("BIT",):
            return data_type, "BitValueType"
        elif data_type in ("CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "CLOB", "TEXT", "LONG", "LONGVARCHAR"):
            return data_type, "StringType"
        elif data_type in ("DATE", "DATETIME", "TIMESTAMP", "TIME"):
            return data_type, "DateAndTimeType"
        return data_type, "UNKNOWN"
