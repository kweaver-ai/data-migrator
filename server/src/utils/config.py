#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from argparse import Namespace
from logging import Logger

import yaml

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.data_classes.mongodb_info import MongodbInfo
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.data_classes.redis_info import RedisInfo, RedisConnectInfo
from dataModelManagement.src.data_classes.opensearch_info import OpenSearchInfo
from dataModelManagement.src.utils.log_util import LogDiy as Log


class LogConfig:
  @classmethod
  def create_logger(cls, parse_args: Namespace) -> Logger:
    if parse_args.log_level:
      return Log.instance().get_logger(parse_args.log_level)
    elif os.environ.get("LOG_LEVEL", "INFO").upper():
      return Log.instance().get_logger(os.environ.get("LOG_LEVEL", "INFO").upper())
    else:
      return Log.instance().get_logger()


class Config:
  logger = None

  def __init__(self, parse_args: Namespace):
    self.parse_args = parse_args

  def load_yaml(self, path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
      yaml_dict = yaml.safe_load(f)
    return yaml_dict

  def load_default_config(self, logger) -> dict:
    if self.parse_args.env_mode is not None and (self.parse_args.env_mode == "dev" or os.environ.get("env_mode") == "dev"):
      default_cfg = self.load_yaml(os.path.join(os.path.dirname(os.path.dirname(__file__)), "config/config.yaml"))
      default_cfg["rds_databases"] = default_cfg["databases"]
    elif self.parse_args.env_mode is not None and (self.parse_args.env_mode == "tiduyun" or os.environ.get("env_mode") == "tiduyun"):
      default_cfg = self.load_yaml("/app/dist/data-model-management/src/config/tiduyun-config.yaml")
      databases_cfg = self.load_yaml("/app/config.yaml")
      logger.info(f"debug point ....print db config:{databases_cfg}")
      default_cfg["renamed_services"] = databases_cfg.get("renamed_services")
      default_cfg["rds_databases"] = databases_cfg["databases"]
      default_cfg["mongodb_databases"] = databases_cfg.get("MONGODB_DATABASES")
    else:
      default_cfg = self.load_yaml("/etc/DataModelManagement/secret.yaml")
      # merge databases config
      databases_cfg = self.load_yaml("/app/config.yaml")
      logger.info(f"debug point ....print db config:{databases_cfg}")
      default_cfg["renamed_services"] = databases_cfg.get("renamed_services")
      default_cfg["rds_databases"] = databases_cfg["databases"]
      default_cfg["mongodb_databases"] = databases_cfg.get("MONGODB_DATABASES")
    return default_cfg

  def merge_config_with_cmd_args(self, cfg: dict) -> dict:
    """merge"""
    # rds
    if self.parse_args.username:
      cfg["depServices"]["rds"]["user"] = self.parse_args.username
    if self.parse_args.password:
      cfg["depServices"]["rds"]["password"] = self.parse_args.password
    if self.parse_args.port:
      cfg["depServices"]["rds"]["port"] = int(self.parse_args.port)
    if self.parse_args.host:
      cfg["depServices"]["rds"]["host"] = self.parse_args.host
    if self.parse_args.host:
      cfg["depServices"]["rds"]["type"] = self.parse_args.type
    if self.parse_args.admin_key:
      cfg["depServices"]["rds"]["admin_key"] = self.parse_args.admin_key
    if self.parse_args.system_id:
      cfg["depServices"]["rds"]["system_id"] = self.parse_args.system_id
    if self.parse_args.source_type:
      cfg["depServices"]["rds"]["source_type"] = self.parse_args.source_type

    # script_path
    if self.parse_args.script_directory_path:
      cfg["config"]["script_directory_path"] = self.parse_args.script_directory_path
    # subparsers
    if not self.parse_args.subcommand or self.parse_args.subcommand == "migrations":
      cfg["config"]["mode"] = self.parse_args.mode if self.parse_args.mode else cfg["config"]["mode"]
      cfg["config"]["online_upgrade"] = self.parse_args.online_upgrade if self.parse_args.online_upgrade else cfg["config"]["online_upgrade"]
      cfg["config"]["stage"] = self.parse_args.stage if self.parse_args.stage else cfg["config"]["stage"]
    return cfg

  def initialize_config_data(self, cfg: dict, logger) -> ConfigData:
    """通过merge好的配置文件，实例化配置类"""
    dep_services = cfg.get("depServices", {})

    rds_data = dep_services.get("rds", None)
    if rds_data is not None:
      rds_info = RDSInfo(**rds_data)
    else:
      rds_info = None

    mongodb_data = dep_services.get("mongodb", None)
    if mongodb_data is not None:
      mongodb_info = MongodbInfo(**mongodb_data)
    else:
      mongodb_info = None

    redis_data = dep_services.get("redis", None)
    if redis_data is not None:
      redis_connect_type = redis_data.get("connectType", "")
      redis_connect_data = redis_data.get("connectInfo", {})
      redis_connect_info = RedisConnectInfo(**redis_connect_data)
      redis_info = RedisInfo(redis_connect_info, redis_connect_type)
    else:
      redis_info = None

    opensearch_data = dep_services.get("opensearch", None)
    if opensearch_data is not None:
      opensearch_info = OpenSearchInfo(**opensearch_data)
    else:
      opensearch_info = None

    config_info = ConfigData(
      rds=rds_info,
      redis=redis_info,
      mongodb=mongodb_info,
      opensearch=opensearch_info,
      logger=logger,
      script_directory_path=cfg["config"]["script_directory_path"],
      mode=cfg["config"]["mode"],
      stage=cfg["config"]["stage"],
      online_upgrade=cfg["config"]["online_upgrade"],
      rds_databases=cfg.get("rds_databases", []),
      mongodb_databases=cfg.get("mongodb_databases", []),
      renamed_services=cfg.get("renamed_services", []),
    )
    return config_info

  def set_environment_variables(self, cfg: ConfigData):
    """部分配置注入环境变量"""
    # 环境变量优先，没有则注入
    os.environ["DB_HOST"] = os.environ["DB_HOST"] if os.environ.get("DB_HOST") else cfg.rds.host if cfg.rds is not None else ""
    os.environ["DB_PORT"] = os.environ["DB_PORT"] if os.environ.get("DB_PORT") else str(cfg.rds.port) if cfg.rds is not None else ""
    os.environ["DB_USER"] = os.environ["DB_USER"] if os.environ.get("DB_USER") else cfg.rds.user if cfg.rds is not None else ""
    os.environ["DB_PASSWD"] = os.environ["DB_PASSWD"] if os.environ.get("DB_PASSWD") else cfg.rds.password if cfg.rds is not None else ""
    os.environ["SYSTEM_ID"] = os.environ["SYSTEM_ID"] if os.environ.get("SYSTEM_ID") else cfg.rds.system_id if cfg.rds is not None else ""

    os.environ["RDS_SOURCE_TYPE"] = cfg.rds.source_type if cfg.rds is not None else ""
    os.environ["ONLINE_UPGRADE"] = cfg.online_upgrade

    os.environ["MONGODB_HOST"] = os.environ["MONGODB_HOST"] if os.environ.get("MONGODB_HOST") else cfg.mongodb.host if cfg.mongodb is not None else ""
    os.environ["MONGODB_PORT"] = os.environ["MONGODB_PORT"] if os.environ.get("MONGODB_PORT") else str(cfg.mongodb.port) if cfg.mongodb is not None else ""
    os.environ["MONGODB_USER"] = os.environ["MONGODB_USER"] if os.environ.get("MONGODB_USER") else cfg.mongodb.user if cfg.mongodb is not None else ""
    os.environ["MONGODB_PASSWORD"] = os.environ["MONGODB_PASSWORD"] if os.environ.get("MONGODB_PASSWORD") else cfg.mongodb.password if cfg.mongodb is not None else ""
    os.environ["MONGODB_AUTH_SOURCE"] = (
      os.environ["MONGODB_AUTH_SOURCE"] if os.environ.get("MONGODB_AUTH_SOURCE") else cfg.mongodb.options.get("authSource") if cfg.mongodb is not None else ""
    )

    os.environ["OPENSEARCH_HOST"] = os.environ["OPENSEARCH_HOST"] if os.environ.get("OPENSEARCH_HOST") else cfg.opensearch.host if cfg.opensearch is not None else ""
    os.environ["OPENSEARCH_PORT"] = os.environ["OPENSEARCH_PORT"] if os.environ.get("OPENSEARCH_PORT") else str(cfg.opensearch.port) if cfg.opensearch is not None else ""
    os.environ["OPENSEARCH_USER"] = os.environ["OPENSEARCH_USER"] if os.environ.get("OPENSEARCH_USER") else cfg.opensearch.user if cfg.opensearch is not None else ""
    os.environ["OPENSEARCH_PASSWORD"] = os.environ["OPENSEARCH_PASSWORD"] if os.environ.get("OPENSEARCH_PASSWORD") else cfg.opensearch.password if cfg.opensearch is not None else ""
    os.environ["OPENSEARCH_PROTOCOL"] = os.environ["OPENSEARCH_PROTOCOL"] if os.environ.get("OPENSEARCH_PROTOCOL") else cfg.opensearch.protocol if cfg.opensearch is not None else ""

  def build_config(self) -> ConfigData:
    """获取一个配置类"""
    logger = LogConfig.create_logger(self.parse_args)
    logger.info("init DataModerManagement tool config start ......")
    default_cfg = self.load_default_config(logger)
    """as的情况"""
    """merge cmd input"""
    cfg = self.merge_config_with_cmd_args(default_cfg)
    logger.info("debug point ....")
    logger.info(cfg)
    config_info = self.initialize_config_data(cfg, logger)
    self.set_environment_variables(config_info)
    return config_info

    # ar的情况
