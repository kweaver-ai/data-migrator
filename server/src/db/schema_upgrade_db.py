#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/4/25 1:04
@Author  : mario.jiang
@File    : schema_upgrade_db.py
"""

import datetime
import json
from logging import Logger

from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.type.rds import RDS
from dataModelManagement.src.db.operate_db import OperateDB


class SchemaUpgradeDB:
  def __init__(self, rds_info: RDSInfo, logger: Logger):
    self.db_info = OperateDB(rds_info, logger)
    self.logger = logger
    self.deploy_db_name = rds_info.get_deploy_db_name()

  def update_service_name(self, old_name: str, new_name: str):
    """更新服务名称"""
    sql = f"update {self.deploy_db_name}.schema_upgrade_table set service_name=%s where service_name=%s"
    return self.db_info.update(sql, new_name, old_name)

  def select_change_task(self, service_name):
    """查询某服务上一次执行的任务"""
    sql = (
      f"select id,service_name,script_file_name,installed_version,target_version,status,create_time,update_time"
      f" from {self.deploy_db_name}.schema_upgrade_table where service_name=%s"
    )
    return self.db_info.fetch_one_result(sql, service_name)

  def update_status_and_target_version(self, status: str, target_version: str, service_name: str):
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"update {self.deploy_db_name}.schema_upgrade_table set status=%s , target_version=%s , update_time=%s where service_name=%s"
    self.logger.debug(sql)
    return self.db_info.update(sql, status, target_version, now, service_name)

  def run_ddl(self, res_sql_list: list):
    return self.db_info.run_ddl(res_sql_list)

  def parse_json_file(self, json_file_path: str, rds: RDS):
    """
    解析json文件，返回ddl语句列表
    """

    with open(json_file_path, "r", encoding="utf-8") as f:
      try:
        update_items = json.load(f)
      except json.JSONDecodeError as e:
        raise Exception(f"无效的JSON文件: {json_file_path}, {e}")
      if not isinstance(update_items, list):
        raise Exception(f"JSON根类型必须为对象(list): {json_file_path}")

      for item in update_items:
        db_name = item["db_name"]
        table_name = item["table_name"]
        object_type = item["object_type"]
        operation_type = item["operation_type"]
        object_name = item.get("object_name", "")
        new_name = item.get("new_name", "")
        object_property = item.get("object_property", "")
        object_comment = item.get("object_comment", "")

        if object_type == "COLUMN":
          if operation_type == "ADD":
            rds.add_column(db_name, table_name, object_name, object_property, object_comment)
          elif operation_type == "MODIFY":
            rds.modify_column(db_name, table_name, object_name, object_property, object_comment)
          elif operation_type == "RENAME":
            rds.rename_column(db_name, table_name, object_name, new_name, object_property, object_comment)
          elif operation_type == "DROP":
            rds.drop_column(db_name, table_name, object_name)
          else:
            raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
        elif object_type == "INDEX" or object_type == "UNIQUE INDEX":
          if operation_type == "ADD":
            rds.add_index(db_name, table_name, object_type, object_name, object_property, object_comment)
          elif operation_type == "RENAME":
            rds.rename_index(db_name, table_name, object_name, new_name)
          elif operation_type == "DROP":
            rds.drop_index(db_name, table_name, object_name)
          else:
            raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
        elif object_type == "CONSTRAINT":
          if operation_type == "ADD":
            rds.add_constraint(db_name, table_name, object_name, object_property)
          elif operation_type == "RENAME":
            rds.rename_constraint(db_name, table_name, object_name, new_name)
          elif operation_type == "DROP":
            rds.drop_constraint(db_name, table_name, object_name)
          else:
            raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
        elif object_type == "TABLE":
          if operation_type == "RENAME":
            rds.rename_table(db_name, table_name, new_name)
          elif operation_type == "DROP":
            rds.drop_table(db_name, table_name)
          else:
            raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")
        elif object_type == "DB":
          if operation_type == "DROP":
            rds.drop_db(db_name)
          else:
            raise Exception(f"不支持的 operation_type '{operation_type}' for object_type '{object_type}': {item}")

  def insert_service_init_schema(self, table: str, columns):
    table_name = f"{self.deploy_db_name}.{table}"
    return self.db_info.insert(table=table_name, columns=columns)

  def update_script_file_name_and_status(self, upgrade_file, status, service, target_version, installed_version):
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")

    sql = (
      f"update {self.deploy_db_name}.schema_upgrade_table set script_file_name=%s, installed_version=%s,"
      f"status=%s,"
      f" target_version=%s, update_time=%s"
      f" where service_name=%s"
    )
    self.logger.debug(sql)
    return self.db_info.update(sql, upgrade_file, installed_version, status, target_version, now, service)

  #
  def update_version_and_status(self, status, service, target_version, installed_version):
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")

    sql = (
      f"update {self.deploy_db_name}.schema_upgrade_table set installed_version=%s,status=%s, target_version=%s, update_time=%s where service_name=%s"
    )
    return self.db_info.update(sql, installed_version, status, target_version, now, service)

  def update_status_and_installed_version(self, status, installed_version, service_name):
    """更新服务状态，已安装版本，更新时间"""
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""update {self.deploy_db_name}.schema_upgrade_table set status = %s, target_version  = %s
        ,update_time=%s, installed_version=%s where service_name=%s"""
    return self.db_info.fetch_one_result(sql, status, installed_version, now, installed_version, service_name)

  def select_post_task(self):
    """查询有post操作需要执行的服务"""
    sql = (
      f"select id,service_name,script_file_name,installed_version,target_version,status,create_time,update_time"
      f" from {self.deploy_db_name}.schema_upgrade_table"
      f" where status=%s or status=%s"
    )
    return self.db_info.fetch_all_result(sql, "pre-success", "post-fail")
