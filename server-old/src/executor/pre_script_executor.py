#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import os
import re
import subprocess
import sys

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.data_classes.directory_structure import ServiceDir
from dataModelManagement.src.data_classes.schema_upgrade import SchemaUpgrade
from dataModelManagement.src.db.init_dm_table_db import InitDMTable
from dataModelManagement.src.db.schema_record_db import SchemaRecordDB
from dataModelManagement.src.db.schema_upgrade_db import SchemaUpgradeDB
from dataModelManagement.src.db.type.dm8 import DM8RDS
from dataModelManagement.src.db.type.goldendb import GoldenDBRDS
from dataModelManagement.src.db.type.kdb9 import KingBaseRDS
from dataModelManagement.src.db.type.mariadb import MariaDBRDS
from dataModelManagement.src.db.type.mysql import MysqlRDS
from dataModelManagement.src.db.type.tidb import TiDBRDS
from dataModelManagement.src.utils.sql_util import SqlUtil
from dataModelManagement.src.utils.util import compare_version, insertion_sort, extract_number


class PreScriptExecutor:
  def __init__(self, cfg: ConfigData):
    self.cfg = cfg
    self.logger = self.cfg.logger
    self.init_dm_table_info = InitDMTable(self.cfg)
    self.sql_util = SqlUtil(self.logger)
    self.schema_upgrade_db = SchemaUpgradeDB(cfg.rds, self.logger)
    self.schema_record_db = SchemaRecordDB(cfg.rds, self.logger)
    self.rds = self.initRDS(cfg.rds)

  def initRDS(self, rds_info: RDSInfo):
    if rds_info.type.lower() == "mariadb":
      return MariaDBRDS(rds_info, self.cfg.logger)
    elif rds_info.type.lower() == "mysql":
      return MysqlRDS(rds_info, self.cfg.logger)
    elif rds_info.type.lower() == "tidb":
      return TiDBRDS(rds_info, self.cfg.logger)
    elif rds_info.type.lower() == "goldendb":
      return GoldenDBRDS(rds_info, self.cfg.logger)
    elif rds_info.type.lower() == "dm8":
      return DM8RDS(rds_info, self.cfg.logger)
    elif rds_info.type.lower() == "kdb9":
      return KingBaseRDS(rds_info, self.cfg.logger)
    else:
      self.logger.error("Unable to recognize database type")
      return None

  def initialize_data_model_database(self):
    """dataModelManagement服务数据初始化"""
    rowlist = []
    deploy_db_name = self.cfg.rds.get_deploy_db_name()
    if self.cfg.rds.type.upper() == "DM8":
      rows = self.init_dm_table_info.select_table_from_deploy_databases_dm(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i["TABLE_NAME"])
    elif self.cfg.rds.type.upper() == "KDB9":
      rows = self.init_dm_table_info.select_table_from_deploy_databases_kdb9(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i["table_name"])
    else:
      rows = self.init_dm_table_info.select_table_from_deploy_databases(deploy_db_name)
      if rows:
        for i in rows:
          rowlist.append(i[f"Tables_in_{deploy_db_name}"])
    self.logger.debug(f"show tables in deploy db:{rowlist}")
    if "schema_upgrade_table" not in rowlist:
      # 初始化创建DM-service表
      self.logger.info("create data_model_table ...")
      init_file_path = self.fetch_service_initialization_file(service_name="data-model-management")
      res_sql_list = self.sql_util.get_target_file_schema(init_file_path)
      self.schema_upgrade_db.run_ddl(res_sql_list)

  def fetch_service_initialization_file(self, service_name: str) -> str:
    service_dir_info = ServiceDir(service_name, self.cfg)
    init_file_path = service_dir_info.get_max_version_init_file()
    return init_file_path

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
    if self.cfg.mode == "latest":
      flag = compare_version(max_version, installed_version) >= 0
    elif self.cfg.mode == "rerun":
      flag = True
    else:
      flag = compare_version(max_version, installed_version) > 0
    if flag:
      # 版本号排序
      sort_version_list = insertion_sort(all_version)
      self.logger.info(f"max version > installed_version, {service} max version is {max_version},old version is {installed_version}")
      # [["xx-1.sql",""xx-2.sql"],["xx-1.sql",""xx-2.sql"]]
      upgrade_files_list = list()
      for version in sort_version_list:
        version_sql_path = os.path.join(db_type_path, version, "pre")
        if self.cfg.mode == "latest":
          flag = compare_version(version, installed_version) >= 0
        elif self.cfg.mode == "rerun":
          flag = True
        else:
          flag = compare_version(version, installed_version) > 0
        if flag and os.path.exists(version_sql_path):
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

  def run_upgrade_file(self, upgrade_file_path: str = ""):
    _, file_extension = os.path.splitext(upgrade_file_path)
    try:
      if file_extension == ".sql":
        # 获取执行sql
        res_sql_list = self.sql_util.get_target_file_schema(upgrade_file_path)
        self.schema_upgrade_db.run_ddl(res_sql_list)

      elif file_extension == ".json":
        self.schema_upgrade_db.parse_json_file(upgrade_file_path, self.rds)

      elif file_extension == ".py":
        # 运行 Python 文件
        venv_python = "python3"

        custom_env = os.environ.copy()
        custom_env["PYTHONUNBUFFERED"] = "1"

        result = subprocess.run([venv_python, upgrade_file_path], env=custom_env, capture_output=True, text=True, check=True, encoding="utf-8")
        self.logger.info(f"运行 {upgrade_file_path} 成功")
        for line in result.stdout.splitlines():
          self.logger.info(line.strip())
        for line in result.stderr.splitlines():
          self.logger.info(line.strip())

    except subprocess.CalledProcessError as ex:
      print(f"[error] 运行 Python 文件失败: {upgrade_file_path}, 错误信息: {ex.stderr}")
      raise Exception(f"运行 Python 文件失败: {upgrade_file_path}")
    except Exception as ex:
      raise Exception(f"execute upgrade file fail, msg: {ex}")

  def record_upgrade_status(self, upgrade_file, service, target_version, installed_version, status: str = "pre-start"):
    """
    升级执行记录方法
    :param upgrade_file:
    :param service:
    :param target_version:
    :param installed_version:
    :param status:
    :return:
    """
    now = datetime.datetime.now()
    now = now.strftime("%Y-%m-%d %H:%M:%S")
    # 更新任务表
    self.schema_upgrade_db.update_script_file_name_and_status(
      upgrade_file=upgrade_file, status=status, service=service, target_version=target_version, installed_version=installed_version
    )
    # 记录任务
    self.schema_record_db.insert_schema_change_record(
      service_name=service, target_version=target_version, installed_version=installed_version, create_time=now, script_file_name=upgrade_file
    )

  def check_and_execute_post_upgrade_task(self, upgrade_files_list, service, target_version):
    """判断是否有post文件存在"""
    for upgrade_files in upgrade_files_list:
      if len(upgrade_files) != 0:
        for upgrade_file_path in upgrade_files:
          version_dir = upgrade_file_path.rsplit("/", 2)[0]
          version_post_dir = os.path.join(version_dir, "post")
          if os.path.isdir(version_post_dir) and len(os.listdir(version_post_dir)) > 0:
            return None
          else:
            pass
    self.schema_upgrade_db.update_version_and_status(
      status="success", service=service, target_version=target_version, installed_version=target_version
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
        if upgrade_record["status"] == "success" or upgrade_record["status"] == "pre-start":
          try:
            self.run_upgrade_file(upgrade_file_path=upgrade_file_path)
          except Exception as ex:
            self.record_upgrade_status(
              upgrade_file=upgrade_file_version,
              service=service,
              status="pre-fail",
              target_version=target_version,
              installed_version=installed_version,
            )
            self.logger.info(f"************{service} service fail run {upgrade_file_version} ************")
            raise Exception(f"Normal upgrade failed, msg:{ex}")
          self.record_upgrade_status(
            upgrade_file=upgrade_file_version, service=service, target_version=target_version, installed_version=installed_version
          )
          self.logger.info(f"************{service} service success run {upgrade_file_version} ************")
        # 前一次执行失败处理
        # 版本与脚本都大于已经安装的，才能进行重试执行
        elif (
          upgrade_record["status"] == "pre-fail"
          and upgrade_file_version.split("/")[0] >= upgrade_record["script_file_name"].split("/")[0]
          and extract_number(upgrade_file) >= extract_number(upgrade_record["script_file_name"])
        ):
          try:
            self.run_upgrade_file(upgrade_file_path=upgrade_file_path)
          except Exception as ex:
            self.record_upgrade_status(
              upgrade_file=upgrade_file_version,
              service=service,
              status="pre-fail",
              target_version=target_version,
              installed_version=installed_version,
            )
            self.logger.info(f"************{service} service fail run {upgrade_file_version} ************")
            raise Exception(f"retry fail upgrade file failed, msg:{ex}")
          self.record_upgrade_status(
            upgrade_file=upgrade_file_version, service=service, target_version=target_version, installed_version=installed_version
          )
          self.logger.info(f"************{service} service success run {upgrade_file_version} ************")
        else:
          pass
    self.schema_upgrade_db.update_status_and_target_version(status="pre-success", target_version=target_version, service_name=service)
    self.check_and_execute_post_upgrade_task(upgrade_files_list, service, target_version)

  def execute_pre_upgrade_stage(self):
    """
    pre阶段执行升级脚本的逻辑
    :return:
    """
    # TODO 读取配置文件，调整service的名称，保证微服务改名后的升级连贯性。
    renamed_services = self.cfg.renamed_services
    if renamed_services:
      for renamed_service in renamed_services:
        old_name = renamed_service["old_name"]
        new_name = renamed_service["new_name"]
        self.logger.info(f"rename service '{old_name}' to '{new_name}'")
        self.schema_upgrade_db.update_service_name(old_name, new_name)

    services = os.listdir(self.cfg.script_directory_path)
    for service in services:
      # 查询数据库中是否含有该服务的安装记录
      service_upgrade_record = self.schema_upgrade_db.select_change_task(service)
      # 存在则升级
      if service_upgrade_record:
        self.logger.info(f"***************************************upgrade {service}, start scanning...")

        # 获取升级文件
        upgrade_files_list, max_version_path, upgrade_flag = self.parse_upgrade_files_for_service(service=service, task_record=service_upgrade_record)

        # 需要升级
        if upgrade_flag:
          self.logger.debug("------------------------------------------------------")
          self.logger.debug(upgrade_files_list)
          self.logger.info(f"***************************************upgrade {service}, run upgrade file...")
          self.execute_upgrade_tasks(upgrade_files_list=upgrade_files_list, service=service, max_version_path=max_version_path)

      # 不存在则安装
      else:
        self.logger.info(f"***************************************install {service} start...")
        init_file_path = self.fetch_service_initialization_file(service_name=service)
        if isinstance(init_file_path, list):
          raise Exception("miss dm-service init file")
        res_sql_list = self.sql_util.get_target_file_schema(init_file_path)
        try:
          self.schema_upgrade_db.run_ddl(res_sql_list)
        except Exception as ex:
          raise Exception(f"init {service} data schema fail:{ex}")
        paths = init_file_path.split("/")
        init_file = paths[-1]
        installed_version = paths[-3]
        now = datetime.datetime.now()
        now = now.strftime("%Y-%m-%d %H:%M:%S")
        schema_upgrade_info = SchemaUpgrade(
          service_name=service,
          script_file_name=init_file,
          installed_version=installed_version,
          target_version=installed_version,
          status="success",
          create_time=now,
          update_time=now,
        )
        col = schema_upgrade_info.to_dict()
        self.schema_upgrade_db.insert_service_init_schema(table="schema_upgrade_table", columns=col)
        self.schema_record_db.insert_schema_change_record(
          service_name=service,
          installed_version=installed_version,
          target_version=installed_version,
          script_file_name=init_file,
          create_time=now,
        )
