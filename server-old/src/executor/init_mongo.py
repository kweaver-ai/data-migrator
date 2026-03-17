#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import base64
import requests
from typing import List

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.utils.log_util import log_response


class MongoDBInit:
  REQUESTS_TIMEOUT = 30.0

  # INIT_DBS = ["anyshare", "osssys", "autosheets", "anydata", "pipeline", "automation"]
  INVALIED_DBS = ["aladdin_cas"]

  API_V2 = {
    "get_user": "/api/proton-mongodb-mgmt/v2/users",
    "delete_database": "/api/proton-mongodb-mgmt/v2/dbs/{database}",
    "create_database": "/api/proton-mongodb-mgmt/v2/dbs/{database}",
    "create_user": "/api/proton-mongodb-mgmt/v2/users/{database}/{user_name}",
    "delete_user": "/api/proton-mongodb-mgmt/v2/users/{database}/{user_name}",
    "patch_role": "/api/proton-mongodb-mgmt/v2/users/{database}/{user_name}/roles",
    "get_databases": "/api/proton-mongodb-mgmt/v2/dbs",
  }

  def __init__(self, cfg: ConfigData):
    self.mongodb_info = cfg.mongodb
    self.logger = cfg.logger
    if cfg.mongodb_databases:
      self.init_dbs = cfg.mongodb_databases
    else:
      self.init_dbs = []
    self.USER_DB = cfg.mongodb.options.get("authSource")

  def url_for(self, host, port, api_name, database="", user_name=""):
    url = "http://{host}:{port}{api}".format(
      port=port,
      host=host,
      api=self.API_V2.get(api_name).format(
        database=database,
        user_name=user_name,
      ),
    )
    self.logger.info(f"print url: {url}")
    return "http://{host}:{port}{api}".format(
      port=port,
      host=host,
      api=self.API_V2.get(api_name).format(
        database=database,
        user_name=user_name,
      ),
    )

  def init_mongodb(self):
    # 获取admin_key的值，该值存在，表示需要执行数据初始化，不存在时，使用外部的db，则初始化语句不执行
    # TODO 后续也需要适配公有云的场景
    if self.mongodb_info.admin_key == "" or self.mongodb_info.source_type == "external":
      return
    if self.init_dbs is None or len(self.init_dbs) == 0:
      self.logger.info("mongodb databases is empty, skip mongodb init")
      return
    self.init_only_create_user()

  def init_only_create_user(self):
    self.logger.info("[PerformanceLogger] (MongoDB)init_only_create_user start")

    # 获取所有数据库
    ok, exist_database_list = self.get_databases(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port)
    self.logger.info(f"mongodb database:{exist_database_list}")
    if not ok:
      raise Exception(f"get databases failed. err: {exist_database_list}")

    # 创建用户数据库
    if self.mongodb_info.options.get("authSource") not in exist_database_list:
      ok, info = self.create_database(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, self.USER_DB)
      if not ok:
        raise Exception(f"create user database {self.mongodb_info.options.get('authSource')} failed. err: {info}")

    # 获取所有用户
    ok, user_list = self.get_all_users(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port)
    self.logger.info(f"mongodb user list:{user_list}")
    if not ok:
      raise Exception(f"mongodb user list. err: {user_list}")

    # 创建用户
    if self.mongodb_info.user not in user_list:
      ok, info = self.create_database_user(
        self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, self.USER_DB, self.mongodb_info.user, self.mongodb_info.password
      )
      if not ok:
        raise Exception(f"create user f{self.mongodb_info.user} failed. err: {info}")

    # 设置权限
    for db in self.init_dbs:
      if db not in exist_database_list and db != self.USER_DB:
        # 建库
        ok, info = self.create_database(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, db)
        if not ok:
          raise Exception(f"create db {db} failed. err: {info}")
    # 授权
    if len(self.init_dbs) > 0:
      # 获取所有权限
      ok, role_db_list = self.get_role_by_user(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, self.mongodb_info.user)
      self.logger.info(f"mongodb user rolo list:{role_db_list}")
      if not ok:
        raise Exception(f"get mongodb user rolo list. err: {role_db_list}")
        # 加权限
      self.init_dbs.append("admin")
      add_role_dbs = list(set(self.init_dbs + role_db_list))
      if not set(add_role_dbs) == set(role_db_list):
        ok, info = self.add_databases_user_role(
          self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, self.USER_DB, self.mongodb_info.user, add_role_dbs
        )
        if not ok:
          raise Exception(f"set user {self.mongodb_info.user} role for dbs {self.init_dbs} failed. err: {info}")

    self.logger.info("[PerformanceLogger] (MongoDB)init_only_create_user succeed")

  def get_role_by_user(self, mongodb_host, mongodb_port, user_name):
    self.logger.info("get db role")
    response = requests.get(
      url=self.url_for(mongodb_host, mongodb_port, "get_user"), headers=self.get_admin_key_header(), timeout=self.REQUESTS_TIMEOUT
    )
    log_response(response)
    if response.status_code in range(200, 300):
      if response.json():
        role_result_list = []
        for db_info in response.json():
          if db_info.get("username") == user_name:
            role_result_list = db_info.get("roles")
            break
        role_list = [role_db.get("db_name") for role_db in role_result_list]
        return True, role_list
      else:
        return True, []
    return False, response.json()

  def get_admin_key_header(self):
    # type: () -> dict
    return {"admin-key": self.mongodb_info.admin_key}

  def create_database(self, mongodb_host, mongodb_port, database):
    # type: (str,int , str) -> (bool, dict)
    response = requests.put(
      url=self.url_for(mongodb_host, mongodb_port, "create_database", database=database),
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    log_response(response)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def delete_database(self, mongodb_host, mongodb_port, database):
    # type: (str,int, str) -> (bool, dict)
    response = requests.delete(
      url=self.url_for(mongodb_host, mongodb_port, "delete_database", database=database),
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    log_msg = "Request URL: {url}, method: {method}, code: {status_code}, content: {response_content}".format(
      url=response.request.url,
      method=response.request.method,
      status_code=response.status_code,
      response_content=response.text,
    )
    self.logger.info(log_msg)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def get_databases(self, mongodb_host, mongodb_port) -> (bool, list):
    response = requests.get(
      url=self.url_for(mongodb_host, mongodb_port, "get_databases"), headers=self.get_admin_key_header(), timeout=self.REQUESTS_TIMEOUT
    )
    log_response(response)
    if response.status_code in range(200, 300):
      return True, [db_info.get("db_name") for db_info in response.json()]
    return False, response.json()

  def get_all_users(self, mongodb_host, mongodb_port):
    response = requests.get(
      url=self.url_for(mongodb_host, mongodb_port, "get_user"), headers=self.get_admin_key_header(), timeout=self.REQUESTS_TIMEOUT
    )
    log_response(response)

    if response.status_code in range(200, 300):
      if response.json():
        return True, [db_info.get("username") for db_info in response.json()]
      else:
        return True, []
    return False, response.json()

  def create_database_user(self, mongodb_host, mongodb_port, database, user_name, password):
    # type: (str,int, str, str, str) -> (bool, dict)
    # requests.delete(
    #     url=self.url_for(mongodb_host, "delete_user", database=database, user_name=user_name),
    #     headers=self.get_admin_key_header(),
    #     timeout=self.REQUESTS_TIMEOUT
    # )
    response = requests.put(
      url=self.url_for(mongodb_host, mongodb_port, "create_user", database=database, user_name=user_name),
      json={"password": base64.b64encode(password.encode("utf-8")).decode("utf-8")},
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    log_response(response)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def delete_user(self, mongodb_host, mongodb_port, database, user_name):
    response = requests.delete(
      url=self.url_for(mongodb_host, mongodb_port, "delete_user", database=database, user_name=user_name),
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def set_database_user_role(self, mongodb_host, mongodb_port, use_db, user_name, database, role):
    # type: (str, int , str, str, str, str) -> (bool, dict)
    # mgmt_host, mgmt_port = self.get_mongodb_mgnt_connenct()
    # url = self.URL_SET_USER_ROLE.format(
    #     host=mgmt_host, port=mgmt_port, database=use_db, user_name=user_name
    # )
    # data = {"roles": [{"db": database, "role": role}]}
    # headers = {"admin-key": self.get_admin_key()}
    url = self.url_for(mongodb_host, mongodb_port, "patch_role", database=use_db, user_name=user_name)
    data = [{"db_name": database, "role": role}]
    headers = self.get_admin_key_header()
    response = requests.put(url=url, json=data, headers=headers, timeout=self.REQUESTS_TIMEOUT)
    log_response(response)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def set_databases_user_role(self, mongodb_host, mongodb_port, use_db, user_name, databases: List[str]):
    # type: (str, int, str, str, List[str]) -> (bool, dict)
    db_roles = [{"db_name": database, "role": "readWrite"} for database in databases]
    db_roles.append({"db_name": "admin", "role": "clusterMonitor"})
    response = requests.put(
      url=self.url_for(mongodb_host, mongodb_port, "patch_role", database=use_db, user_name=user_name),
      json=db_roles,
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    log_response(response)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def add_databases_user_role(self, mongodb_host, mongodb_port, use_db, user_name, databases: List[str]):
    # type: (str,int, str, str, List[str]) -> (bool, dict)
    db_roles = [{"db_name": database, "role": "readWrite"} for database in databases]
    self.logger.info(f"add role db:{db_roles}")
    for item in db_roles:
      if item.get("db_name") == "admin":
        item["role"] = "clusterMonitor"
    response = requests.patch(
      url=self.url_for(mongodb_host, mongodb_port, "patch_role", database=use_db, user_name=user_name),
      json=db_roles,
      headers=self.get_admin_key_header(),
      timeout=self.REQUESTS_TIMEOUT,
    )
    log_response(response)
    if response.status_code in range(200, 300):
      return True, {}
    return False, response.json()

  def add_mongodb_database(self, databases: List[str], anyshare_namespace="anyshare"):
    if not databases:
      raise Exception("Databases is Empty")
    db_exists_code = 403025006
    dbs = set(databases)
    dbs = list(dbs)
    for db in dbs:
      ok, info = self.create_database(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, db)
      if not ok:
        if info.get("code", 0) == db_exists_code:
          self.logger.info("db[{}] already exists".format(db))
          continue
        raise Exception(f"create user database {self.USER_DB} failed. err: {info}")

    ok, info = self.add_databases_user_role(self.mongodb_info.mgmt_host, use_db=self.USER_DB, user=self.mongodb_info.user, databases=dbs)
    if not ok:
      raise Exception(f"add user {self.mongodb_info.user} role for dbs {dbs} failed. err: {info}")

  def save_mongodb_info(self, mongodb_info):
    if not mongodb_info:
      self.logger.info("No MongoDB Info To Save.")
      return

  def user_is_admin(self, user_name):
    # type: (str) -> bool
    return user_name == self.mongodb_info.user

  def delete_invalid_databases(self):
    for db in self.INVALIED_DBS:
      self.delete_database(self.mongodb_info.mgmt_host, self.mongodb_info.mgmt_port, db)
