#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/4/24 15:29
@Author  : mario.jiang
@File    : operate_db.py
"""
import re
from logging import Logger

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.connection import DatabaseConnection


class OperateDB(object):

    def __init__(self, rds_info: RDSInfo, logger: Logger):
        self.rds_info = rds_info
        database_connection = DatabaseConnection(self.rds_info)
        self.conn = database_connection.get_conn()
        self.logger = logger

    def __del__(self):
        """
        Del
        """
        self.conn.close()

    def get_columns(self, columns):
        """
        将列名列表转换为字符串
        param columns : 由表中的列名组成的列表
        例如：
            get_columns(["id", "name"])
            => (`id`, `name`)
        """
        stmt = ["`%s`" % c for c in columns]
        return " (%s) " % (", ".join(stmt))

    def run_ddl(self, res_sql_list: list):
        """
        执行ddl语句的方法
        res_sql_list： sql语句组合的列表
        """
        execute_sql = ""
        try:
            self.cursor = self.conn.cursor()
            if len(res_sql_list) > 0:
                for index, sql_item in enumerate(res_sql_list):
                    self.logger.info(f"Execute the {index}th sentence")
                    execute_sql = self.transform_sql_query(sql_item)
                    self.logger.info(f"{execute_sql}")
                    self.cursor.execute(execute_sql)
                self.conn.commit()
        except Exception as ex:
            self.conn.rollback()
            self.logger.error(f"execute_sql is {execute_sql}")
            raise ex
        finally:
            self.cursor.close()

    def transform_sql_query(self, sql: str) -> str:
        """
        对应多租户情况下，建库需要额外增加前缀名。
        """
        system_id = self.rds_info.system_id
        patterns = [
            ("^USE\\s+(.*);$", "USE"),
            ("^SET SCHEMA\\s+(.*);$", "SET SCHEMA")
        ]

        for pattern, command in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.search(sql)

            if match:
                dbname = match.group(1)
                # 适配use的情况
                if command == "USE":
                    dbname = dbname.replace(" ", "").replace("`", "")
                    new_dbname = f"`{system_id}{dbname}`"
                    return f"{command} {new_dbname};"
                # 适配set schema的情况
                dbname = dbname.replace(" ", "").replace('"', '')
                new_dbname = f'"{system_id}{dbname}"'
                return f"{command} {new_dbname};"
        return sql

    def insert(self, table: str, columns):
        """
                插入一条数据,建议使用字典方式构造,可读性强
                若列表方式,只适用于对表中所有字段进行插入
                Args:
                    table: string，要插入的表名
                    columns: dict or list，要插入值的列名以及值
                             如果是字典，键为列名
                             如果是列表，元素为值
                Return:
                    最后一个插入语句的自增ID，如果没有则为0
                Raise:
                    TypeError: 参数类型错误时丢出异常
                Example:
                    insert("test_table", {"id": 1, "name": "test"})
                    => INSERT INTO `test_table` (`id`, `name`) VALUES ("1", "test")
                    insert("test_table", [1, "test"])
                    => INSERT INTO `test_table` VALUES ("1", "test")
                """
        if not isinstance(table, str):
            raise TypeError("table only use string type")

        sql = ["INSERT INTO {0} ".format(table)]

        if isinstance(columns, dict):
            sql.append(self.get_columns(list(columns.keys())))
            values = list(columns.values())
        elif isinstance(columns, list):
            values = columns
        else:
            raise TypeError("columns only use list or dict type")

        sql.append(" VALUES (%s) " % (", ".join(["%s"] * len(values))))

        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute("".join(sql), values)
            self.conn.commit()
        except Exception as ex:
            self.conn.rollback()
            raise ex
        finally:
            self.cursor.close()

    def fetch_one_result(self, sql, *args):
        """
        功能：执行一条查询语句，并返回一条记录
        举例：
            db_obj = Connector(dbinfo)
            conn = db_obj.get_db_operate_obj()
            sql = "SELECT `name` FROM `table`
                WHERE `id` = %s"
            result = conn.fetch_one_result(sql, id)
        """
        try:
            self.cursor = self.conn.cursor()
            self.logger.debug(f"{sql} ||| {args}")
            self.cursor.execute(sql, args)
            result = self.cursor.fetchone()
            self.conn.commit()
            return result
        except Exception as ex:
            self.conn.rollback()
            raise ex
        finally:
            self.cursor.close()

    def update(self, sql, *args):
        """
        功能：执行一条更新的sql
        说明：本操作返回受影响行数与最后插入行的自增ID,
              当格式化参数为数值型时,依然使用 %s(MySQLdb 格式化的一个问题),如下所示：
        举例：
            db_obj = Connector(dbinfo)
            conn = db_obj.get_db_operate_obj()
            sql = "UPDATE `test_table` SET `name` = %s
            WHERE `id` = %s"
            调用 conn.update(sql, name, 1000)
        """
        try:
            self.cursor = self.conn.cursor()
            affect_row = self.cursor.execute(sql, args)
            self.conn.commit()
            return affect_row
        except Exception as ex:
            self.conn.rollback()
            raise ex
        finally:
            self.cursor.close()

    def insert_many(self, table, columns, values):
        """
        插入多条数据,建议使用字典方式构造,可读性强
        若列表方式,只适用于对表中所有字段进行插入
        Args:
            table: 字符串，要插入的表名
            columns: 列表，要插入值的列，不需要参数使用空列表
            values: 列表元组嵌套，要插入的值
        Return:
            插入行数
        Raise:
            TypeError: 参数类型错误时丢出异常
        Example:
            insert_many("test_table", ["id", "name"], [(1, "name1"), (2, "name2")])
            => INSERT INTO `test_table` (`id`, `name`) VALUES ("1", "name1"), ("2", "name2")
            insert_many("test_table", [], [(1, "name1"), (2, "name2")])
            => INSERT INTO `test_table` VALUES ("1", "name1"), ("2", "name2")
        """
        if not isinstance(table, str):
            raise TypeError("table only use string type")

        if not isinstance(columns, list) or not isinstance(values, list):
            raise TypeError("columns or values only use list type")

        if not isinstance(values[0], tuple):
            raise TypeError("values value only use tuple type")

        sql = ["INSERT INTO {0}".format(table)]
        if columns:
            sql.append(self.get_columns(columns))

        sql.append(" VALUES (%s) " % (", ".join(["%s"] * len(values[0]))))

        try:
            self.cursor = self.conn.cursor()
            row_affected = self.cursor.executemany("".join(sql), values)
            self.conn.commit()
            return row_affected
        except Exception as ex:
            self.conn.rollback()
            raise ex
        finally:
            self.cursor.close()

    def fetch_all_result(self, sql, *args):
        """
        功能：执行一条查询语句，并返回所有结果
        举例：
            db_obj = Connector(dbinfo)
            conn = db_obj.get_db_operate_obj()
            sql = "SELECT `name` FROM `table`"
            result = conn.fetch_all_result(sql)
        """
        try:
            self.cursor = self.conn.cursor()
            self.cursor.execute(sql, args)
            result = self.cursor.fetchall()
            self.conn.commit()
            return result
        except Exception as ex:
            self.conn.rollback()
            raise ex
        finally:
            self.cursor.close()
