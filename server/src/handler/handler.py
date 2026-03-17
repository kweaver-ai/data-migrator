#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from argparse import Namespace

from dataModelManagement.src.executor.init_db import InitDatabase
from dataModelManagement.src.executor.init_mongo import MongoDBInit
from dataModelManagement.src.executor.post_script_executor import PostScriptExecutor
from dataModelManagement.src.executor.pre_script_executor import PreScriptExecutor

# from dataModelManagement.src.executor.verification_executer import VerificationExecutor
from dataModelManagement.src.utils.config import Config


class Handler:
  def __init__(self, parse_args: Namespace):
    self.parse_args = parse_args

  def migration_run(self):
    config_info = Config(self.parse_args)
    cfg = config_info.build_config()
    logger = cfg.logger
    logger.info("init config success")
    if cfg.stage == "pre":
      logger.info("DataModelManagementPre service start ...")

      if cfg.rds is not None:
        logger.info("init database start")
        database_init = InitDatabase(cfg)
        database_init.init_databases()
        logger.info("init database success")

      if cfg.mongodb is not None:
        logger.info("init mongodb start")
        mongodb_init = MongoDBInit(cfg)
        mongodb_init.init_mongodb()
        logger.info("init mongodb success")

      logger.info("init data_model_management table start")
      # database_init
      pre_executor = PreScriptExecutor(cfg)
      pre_executor.initialize_data_model_database()
      logger.info("init data_model_management table success")

      logger.info("start pre stage")
      pre_executor.execute_pre_upgrade_stage()
      logger.info("success pre stage")
      logger.info("DataModelManagementPre service success ...")
    elif cfg.stage == "post":
      logger.info("DataModelManagementPost service start ...")
      logger.info("start post-upgrade task...")
      post_executor = PostScriptExecutor(cfg)
      post_executor.execute_post_upgrade_stage()
      logger.info("post-upgrade task success...")
      logger.info("DataModelManagementPost service success ...")

  def verification_run(self):
    config_info = Config(self.parse_args)
    cfg = config_info.build_config()
    cfg.logger.info("start verify stage")
    cfg.logger.info("do nothing and return")
    # if os.getenv("DB_TYPE").upper() == "DM8" or os.getenv("DB_TYPE").upper() == "KDB9":
    #     cfg.logger.info("The current tool does not support the DM8 and KDB9 database")
    #     cfg.logger.info("success execute")
    # else:
    #     verification_executor = VerificationExecutor(cfg)
    #     verification_executor.run()
    cfg.logger.info("verify stage execute success")
