#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import requests
import time
from typing import List

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.db.root_query import RDSRootQuery
from dataModelManagement.src.db.type.dm8 import DM8RDS
from dataModelManagement.src.db.type.goldendb import GoldenDBRDS
from dataModelManagement.src.db.type.kdb9 import KingBaseRDS
from dataModelManagement.src.db.type.mariadb import MariaDBRDS
from dataModelManagement.src.db.type.mysql import MysqlRDS
from dataModelManagement.src.db.type.tidb import TiDBRDS


class InitDatabase:
  # mgnt
  REQUESTS_TIMEOUT = 30.0
  MARIADB_DB_URL_V2 = "http://{host}:{port}/api/proton-rds-mgmt/v2/dbs/{dbname}"
  MARIADB_USER_URL_V2 = "http://{host}:{port}/api/proton-rds-mgmt/v2/users/{user_name}"
  MARIADB_USER_PRIVILEGE_URL_V2 = "http://{host}:{port}/api/proton-rds-mgmt/v2/users/{user_name}/privileges"
  USER_PRIVILEGE = "ReadWrite"
  # MARIADB_MGMT_HOST = "mariadb-mgmt-cluster.resource"
  # # MARIADB_MGMT_HOST = "10.96.85.189"
  # MARIADB_MGMT_PORT = "8888"

  def __init__(self, cfg: ConfigData):
    self.cfg = cfg
    self.logger = self.cfg.logger

  def init_databases(self):
    """
    以deploy库为标识建库,建库
    :return:
    """

    # 获取admin_key的值，该值存在，表示需要执行数据初始化，不存在时，使用外部的db，则初始化语句不执行
    # TODO 后续也需要适配公有云的场景
    if self.cfg.rds.source_type == "external" and self.cfg.rds.admin_key != "":
      self.init_external_rds_task()
      return
    elif self.cfg.rds.admin_key == "":
      self.logger.info("admin_key is null, pass init db")
      return
    self.init_internal_rds_task()
    return

  def init_external_rds_task(self):
    self.logger.info("start external rds init")
    rds = None
    new_databases = set()
    if self.cfg.rds.type.lower() == "mariadb":
      rds = MariaDBRDS(self.cfg.rds, self.cfg.logger)
    elif self.cfg.rds.type.lower() == "mysql":
      rds = MysqlRDS(self.cfg.rds, self.cfg.logger)
    elif self.cfg.rds.type.lower() == "tidb":
      rds = TiDBRDS(self.cfg.rds, self.cfg.logger)
    elif self.cfg.rds.type.lower() == "goldendb":
      rds = GoldenDBRDS(self.cfg.rds, self.cfg.logger)
    elif self.cfg.rds.type.lower() == "dm8":
      rds = DM8RDS(self.cfg.rds, self.cfg.logger)
    elif self.cfg.rds.type.lower() == "kdb9":
      rds = KingBaseRDS(self.cfg.rds, self.cfg.logger)
    else:
      self.logger.error("Unable to recognize database type")
      return

    if rds:
      # 查询配置中指定的用户是否存在
      result = rds.select_user_form_database(user_name=self.cfg.rds.user)
      if result:
        self.logger.info("create new db")
        # 0.初始化数据库配置
        rds.init_db_config()
        # 1.是否有新增的数据库,有则创建
        exist_db_list = rds.select_all_databases_user()
        self.logger.info(f"external exist db list : {exist_db_list}")
        for db in self.cfg.rds_databases:
          db = self.cfg.rds.system_id + db
          if db not in exist_db_list:
            self.logger.info(f"add db: {db}")
            rds.create_db(db)
            new_databases.add(db)
        # 2. 授权
        self.logger.info(f"add db list :{new_databases}")
        if len(new_databases) > 0:
          rds.add_user_privilege(user_name=self.cfg.rds.user, databases=new_databases)
          self.logger.info(f"add rw privilege db list :{new_databases}")
      else:
        self.logger.info("No new users created.")
        # 0.初始化数据库配置
        rds.init_db_config()
        # 1.初始化数据库
        exist_db_list = rds.select_all_databases_root()
        self.logger.info(f"external exist db list : {exist_db_list}")
        for db in self.cfg.rds_databases:
          db = self.cfg.rds.system_id + db
          if db not in exist_db_list:
            rds.create_db(db)
            new_databases.add(db)
            self.logger.info(f"add db: {db}")
        # 2.创建用户
        rds.create_user(user_name=self.cfg.rds.user, password=self.cfg.rds.password)
        self.logger.info(f"create user :{self.cfg.rds.user}")
        # 3.授权
        rds.set_user_privilege(user_name=self.cfg.rds.user, databases=new_databases)
        self.logger.info(f"add rw privilege db list :{new_databases}")
        # 4.初始化数据库一些函数等，建库完成后的工作
        rds.init_db_after()

  def init_internal_rds_task(self):
    new_databases = set()
    rds_root_query = RDSRootQuery(self.cfg.rds)
    rows = rds_root_query.select_all_databases()
    rowlist = []
    for i in rows:
      rowlist.append(i["Database"])

    # 查询配置中指定的用户是否存在
    result = rds_root_query.select_user_form_database(user_name=self.cfg.rds.user)
    if result:
      self.logger.info("No new users created.")
      # 是否有新增的数据库,有则创建
      for db in self.cfg.rds_databases:
        db = self.cfg.rds.system_id + db
        if db not in rowlist:
          self.create_db(db)
          new_databases.add(db)
      if len(new_databases) > 0:
        self.add_user_privilege(user_name=self.cfg.rds.user, databases=new_databases)
    else:
      self.logger.info("create new db_user.")
      # 1.初始化数据库
      for db in self.cfg.rds_databases:
        db = self.cfg.rds.system_id + db
        if db not in rowlist:
          self.create_db(db)
          new_databases.add(db)
      # 2.创建用户
      self.create_user(user_name=self.cfg.rds.user, password=self.cfg.rds.password)
      # 3.授权
      self.set_user_privilege(user_name=self.cfg.rds.user, add_dbs=new_databases)

  def create_db(self, db_name):
    """创建数据库"""
    log_prefix = "Create MariaDB DB %s" % db_name
    self.cfg.logger.info(log_prefix)
    url = self.MARIADB_DB_URL_V2.format(host=self.cfg.rds.mgmt_host, port=self.cfg.rds.mgmt_port, dbname=db_name)
    admin_key = self.cfg.rds.admin_key
    headers = {"admin-key": admin_key}
    data = {"charset": "utf8mb4", "collate": "utf8mb4_unicode_ci"}
    retry = 5
    while retry > 0:
      try:
        log_msg = log_prefix + ": url:%s, method:PUT, data:%s" % (
          url,
          str(data),
        )
        self.logger.info(log_msg)
        response = requests.put(url, headers=headers, json=data, timeout=self.REQUESTS_TIMEOUT)

      except Exception as exp:
        err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=exp, retry=retry)
        self.logger.debug(err_msg)
        retry -= 1
        if retry == 0:
          raise Exception(err_msg)
        time.sleep(10)
        continue
      else:
        if response.status_code not in range(200, 300):
          err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=response.content, retry=retry)
          self.logger.debug(err_msg)
          retry -= 1
          if retry == 0 or response.status_code in (400, 403):
            if response.json().get("code", 0) == 403012006:
              self.logger.info("db[{}] already exists".format(db_name))
              break
            raise Exception(err_msg)
          time.sleep(10)
          continue
        break

  def create_user(self, user_name, password):
    """创建用户"""
    log_prefix = "Create MariaDB user %s" % user_name
    self.logger.info(log_prefix)
    url = self.MARIADB_USER_URL_V2.format(host=self.cfg.rds.mgmt_host, port=self.cfg.rds.mgmt_port, user_name=user_name)
    admin_key = self.cfg.rds.admin_key
    headers = {"admin-key": admin_key}
    data = {"password": base64.b64encode(password.encode("utf-8")).decode("utf-8")}
    retry = 5
    while retry > 0:
      try:
        log_msg = log_prefix + ": url:%s, method:PUT, data:%s" % (
          url,
          str(data),
        )
        self.logger.info(log_msg)
        response = requests.put(url, headers=headers, json=data, timeout=self.REQUESTS_TIMEOUT)
      except Exception as exp:
        err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=exp, retry=retry)
        self.logger.debug(err_msg)
        retry -= 1
        if retry == 0:
          raise Exception(err_msg)
        time.sleep(10)
        continue
      else:
        if response.status_code not in range(200, 300):
          err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=response.content, retry=retry)
          self.logger.debug(err_msg)
          retry -= 1
          if retry == 0 or response.status_code == 400:
            raise Exception(err_msg)
          time.sleep(10)
          continue
        break

  def set_user_privilege(self, user_name, add_dbs):
    """给指定用户授权"""
    log_prefix = "Set MariaDB user[%s] privilege" % user_name
    self.logger.info(log_prefix)
    url = self.MARIADB_USER_PRIVILEGE_URL_V2.format(host=self.cfg.rds.mgmt_host, port=self.cfg.rds.mgmt_port, user_name=user_name)
    admin_key = self.cfg.rds.admin_key
    headers = {"admin-key": admin_key}
    data = self._get_user_privilege_body(add_dbs)
    retry = 5
    while retry > 0:
      try:
        log_msg = log_prefix + ": url:%s, method:PUT, data:%s" % (
          url,
          str(data),
        )
        self.logger.info(log_msg)
        response = requests.put(url, headers=headers, json=data, timeout=self.REQUESTS_TIMEOUT)
      except Exception as exp:
        err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=exp, retry=retry)
        self.logger.debug(err_msg)
        retry -= 1
        if retry == 0:
          raise Exception(err_msg)
        time.sleep(10)
        continue
      else:
        if response.status_code not in range(200, 300):
          err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=response.content, retry=retry)
          self.logger.debug(err_msg)
          retry -= 1
          if retry == 0 or response.status_code == 400:
            raise Exception(err_msg)
          time.sleep(10)
          continue
        break

  def _get_user_privilege_body(self, dbs):
    privilege_body = list()
    for db_name in dbs:
      privilege = {"db_name": db_name, "privilege_type": self.USER_PRIVILEGE}
      privilege_body.append(privilege)
    return privilege_body

  def add_user_privilege(self, user_name: str, databases: List[str]):
    log_prefix = "Add MariaDB user[%s] privilege" % user_name
    self.logger.info(log_prefix + ", db[%s], privilege[%s]" % (databases, self.USER_PRIVILEGE))
    url = self.MARIADB_USER_PRIVILEGE_URL_V2.format(host=self.cfg.rds.mgmt_host, port=self.cfg.rds.mgmt_port, user_name=user_name)

    admin_key = self.cfg.rds.admin_key
    headers = {"admin-key": admin_key}
    data = self._get_user_privilege_body(databases)
    retry = 5
    while retry > 0:
      try:
        log_msg = log_prefix + ": url:%s, method:PATCH, data:%s" % (
          url,
          str(data),
        )
        self.logger.info(log_msg)
        response = requests.patch(url, headers=headers, json=data, timeout=self.REQUESTS_TIMEOUT)
      except Exception as exp:
        err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=exp, retry=retry)
        self.logger.debug(err_msg)
        retry -= 1
        if retry == 0:
          raise Exception(err_msg)
        time.sleep(10)
        continue
      else:
        if response.status_code not in range(200, 300):
          err_msg = "{prefix} failed, err: {err}, retry: {retry}".format(prefix=log_prefix, err=response.content, retry=retry)
          self.logger.debug(err_msg)
          retry -= 1
          if retry == 0 or response.status_code == 400:
            raise Exception(err_msg)
          time.sleep(10)
          continue
        break

  def get_rds_db_type(self):
    return self.cfg.rds.type
