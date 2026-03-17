#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import subprocess
import sys

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.data_classes.directory_structure import ServiceDir
from dataModelManagement.src.db.init_dm_table_db import InitDMTable
from dataModelManagement.src.db.schema_record_db import SchemaRecordDB
from dataModelManagement.src.db.schema_upgrade_db import SchemaUpgradeDB
from dataModelManagement.src.utils.sql_util import SqlUtil
from dataModelManagement.src.utils.util import insertion_sort, compare_version, extract_number


class PostScriptExecutor:
  def __init__(self, cfg: ConfigData):
    self.cfg = cfg
    self.logger = self.cfg.logger
    self.sql_util = SqlUtil(self.logger)
    self.schema_upgrade_db = SchemaUpgradeDB(cfg.rds, self.logger)
    self.schema_record_db = SchemaRecordDB(cfg.rds, self.logger)
    self.init_dm_table_info = InitDMTable(self.cfg)

  def parse_upgrade_files_for_service(self, service: str = "", task_record: dict = None):
    """
    根据服务名，解析升级文件
    :return:
    """
    service_dir_info = ServiceDir(service, self.cfg)
    # 数据类型目录
    db_type_path = service_dir_info.database_type_dir_info.path
    # 获取所有版本号
    all_version = service_dir_info.database_type_dir_info.get_all_version()
    # 获取最大版本号
    max_version = service_dir_info.database_type_dir_info.get_max_version_info().version
    # 最大版本号路径
    max_version_path = service_dir_info.database_type_dir_info.get_max_version_info().path
    # 获取已安装版本
    installed_version = task_record["installed_version"]
    self.logger.debug(f"======{service}======")
    self.logger.debug("------------------------------------------------------")
    self.logger.info(f"old version {installed_version}  ||||||||||||  new version {max_version}")
    # 版本号比较 1大于，0等于，-1小于
    if compare_version(max_version, installed_version) == 1:
      # 版本号排序
      sort_version_list = insertion_sort(all_version)
      self.logger.info(f"max version > installed_version, {service} max version is {max_version},old version is {installed_version}")
      # [["1-xx.sql",""2-xx.sql"],["1-xxx.sql",""2-xxx.sql"]]
      upgrade_files_list = list()
      for version in sort_version_list:
        version_sql_path = os.path.join(db_type_path, version, "post")
        if compare_version(version, installed_version) == 1 and os.path.exists(version_sql_path):
          upgrade_files = []
          for filename in os.listdir(version_sql_path):
            # 匹配.sql
            match1 = re.match(r"^\d+-.*\.sql$", filename)
            # 匹配.py
            match2 = re.match(r"^\d+-.*\.py$", filename)
            # 匹配.json
            match3 = re.match(r"^\d+-.*\.json$", filename)
            if match1 or match2 or match3:
              upgrade_files.append(os.path.join(version_sql_path, filename))

          if len(upgrade_files) != 0:
            # 排序升级文件
            sort_upgrade_files = sorted(upgrade_files, key=extract_number)
            upgrade_files_list.append(sort_upgrade_files)
      if len(upgrade_files_list) == 0:
        return upgrade_files_list, "", False
      return upgrade_files_list, max_version_path, True
    else:
      return [], "", False

  def run_upgrade_file(self, upgrade_file_path):
    _, file_extension = os.path.splitext(upgrade_file_path)
    try:
      if file_extension == ".sql":
        # 获取执行sql
        res_sql_list = self.sql_util.get_target_file_schema(upgrade_file_path)
        self.schema_upgrade_db.run_ddl(res_sql_list)

      elif file_extension == ".py":
        # 运行 Python 文件
        venv_python = "python3"

        custom_env = os.environ.copy()
        custom_env["PYTHONUNBUFFERED"] = "1"

        result = subprocess.run([venv_python, upgrade_file_path], env=custom_env, capture_output=True, text=True, check=True, encoding="utf-8")
        print(f"运行 {upgrade_file_path} 成功, result: {result}")

        for line in result.stdout.splitlines():
          self.logger.info(line.strip())

    except subprocess.CalledProcessError as ex:
      print(f"[error] 运行 Python 文件失败: {upgrade_file_path}, 错误信息: {ex.stderr}")
      raise Exception(f"运行 Python 文件失败: {upgrade_file_path}")
    except Exception as ex:
      raise Exception(f"execute upgrade file fail, msg: {ex}")

  def record_upgrade_status(self, upgrade_file, service, target_version, installed_version, status: str = "post-start"):
    """
    升级执行记录方法
    :param upgrade_file:
    :param service:
    :param target_version:
    :param installed_version:
    :param status:
    :return:
    """
    # 更新任务表
    self.schema_upgrade_db.update_script_file_name_and_status(
      upgrade_file=upgrade_file, status=status, service=service, target_version=target_version, installed_version=installed_version
    )
    # 记录任务
    self.schema_record_db.insert_schema_change_record(
      service_name=service, target_version=target_version, installed_version=installed_version, script_file_name=upgrade_file
    )

  def execute_upgrade_tasks(self, upgrade_files_list: list, service: str, max_version_path: str):
    """
    升级处理逻辑
    :return:
    """
    # 获取
    max_version_paths = max_version_path.split("/")
    target_version = max_version_paths[-1]
    # 执行升级文件
    self.logger.debug(f"********************{max_version_path}*******************")
    service_upgrade_record = self.schema_upgrade_db.select_change_task(service)
    self.logger.debug(f"----{service_upgrade_record}---")
    installed_version = service_upgrade_record["installed_version"]
    for files in upgrade_files_list:
      for upgrade_file_path in files:
        upgrade_record = self.schema_upgrade_db.select_change_task(service)
        parts = upgrade_file_path.split("/")
        upgrade_file = parts[-1]
        upgrade_file_version = "/".join(parts[-3:])
        self.logger.info(f"[{service}]execution of the {upgrade_file_path} execution of the file")
        self.logger.info(f"{service} service last time status is {upgrade_record['status']} and script is {upgrade_record['script_file_name']}")
        # 正常执行脚本
        if upgrade_record["status"] == "pre-success" or upgrade_record["status"] == "post-start":
          try:
            self.run_upgrade_file(upgrade_file_path=upgrade_file_path)
          except Exception as ex:
            self.record_upgrade_status(
              upgrade_file=upgrade_file_version,
              service=service,
              status="post-fail",
              target_version=target_version,
              installed_version=installed_version,
            )
            self.logger.info(f"************{service} service fail run {upgrade_file_version} ************")
            raise Exception(f"Normal upgrade failed, msg:{ex}")
          self.record_upgrade_status(
            upgrade_file=upgrade_file_version, service=service, target_version=target_version, installed_version=target_version
          )
          self.logger.info(f"************{service} service success run {upgrade_file_version} ************")
        # 前一次执行失败处理
        # 版本与脚本都大于已经安装的，才能进行重试执行
        elif (
          upgrade_record["status"] == "post-fail"
          and upgrade_file_version.split("/")[0] >= upgrade_record["script_file_name"].split("/")[0]
          and extract_number(upgrade_file) >= extract_number(upgrade_record["script_file_name"])
        ):
          try:
            self.run_upgrade_file(upgrade_file_path=upgrade_file_path)
          except Exception as ex:
            self.record_upgrade_status(
              upgrade_file=upgrade_file_version,
              service=service,
              status="post-fail",
              target_version=target_version,
              installed_version=installed_version,
            )
            self.logger.info(f"************{service} service fail run {upgrade_file_version} ************")
            raise Exception(f"retry fail upgrade file failed, msg:{ex}")
          self.record_upgrade_status(
            upgrade_file=upgrade_file_version, service=service, target_version=target_version, installed_version=target_version
          )
          self.logger.info(f"************{service} service success run {upgrade_file_version} ************")
        else:
          pass
    self.schema_upgrade_db.update_status_and_target_version(status="success", target_version=target_version, service_name=service)

  def execute_post_upgrade_stage(self):
    """dataModelManagementPost服务数据初始化"""
    deploy_db_name = self.cfg.rds.get_deploy_db_name()
    rowlist = []
    if self.cfg.rds.type.lower() == "dm8":
      rows = self.init_dm_table_info.select_table_from_deploy_databases_dm(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i["TABLE_NAME"])
    elif self.cfg.rds.type.lower() == "kdb9":
      rows = self.init_dm_table_info.select_table_from_deploy_databases_kdb9(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i["table_name"])
    else:
      rows = self.init_dm_table_info.select_table_from_deploy_databases(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i[f"Tables_in_{deploy_db_name}"])
    if "schema_upgrade_table" in rowlist:
      pass
    else:
      raise Exception("pre dataModelManagement Not executed!")

    # 获取本镜像内所有进行了pre-success操作的微服务记录
    pre_success_services = self.intersect_lists()

    if pre_success_services:
      for service_upgrade_record in pre_success_services:
        self.logger.info(f"********* upgrade {service_upgrade_record['service_name']}, start scanning...")
        # 获取升级文件
        upgrade_files_list, max_version_path, is_post_upgrade = self.parse_upgrade_files_for_service(
          service=service_upgrade_record["service_name"], task_record=service_upgrade_record
        )
        # 需要升级
        if is_post_upgrade:
          self.logger.debug("------------------------------------------------------")
          self.logger.debug(upgrade_files_list)
          self.logger.info(f"***************************************upgrade {service_upgrade_record['service_name']}, run upgrade file...")
          self.execute_upgrade_tasks(
            upgrade_files_list=upgrade_files_list, service=service_upgrade_record["service_name"], max_version_path=max_version_path
          )
        else:
          self.schema_upgrade_db.update_status_and_installed_version(
            status="success", installed_version=service_upgrade_record["target_version"], service_name=service_upgrade_record["service_name"]
          )
          self.logger.info(f"{service_upgrade_record['service_name']} service no post execute...")
    # 本次升级不需要执行post
    else:
      self.logger.info("no post execute...")

  def intersect_lists(self) -> list:
    """
    1. 获取数据库中，状态为pre-success的服务
    2. 获取本镜像内的数据库升级脚本
    3. 取交集返回
    目的是使得post只处理本镜像内的升级脚本
    """
    # 获取所有进行了pre-success操作的微服务记录
    pre_success_services = self.schema_upgrade_db.select_post_task()
    exist_services = os.listdir(self.cfg.script_directory_path)
    services = []
    for pre_success_service in pre_success_services:
      if pre_success_service["service_name"] in exist_services:
        services.append(pre_success_service)
    return services
