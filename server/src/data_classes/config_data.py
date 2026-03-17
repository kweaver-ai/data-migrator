#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List

from dataModelManagement.src.data_classes.mongodb_info import MongodbInfo
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.data_classes.redis_info import RedisInfo
from dataModelManagement.src.data_classes.opensearch_info import OpenSearchInfo
from dataModelManagement.src.utils.log_util import Logger as Log


@dataclass
class ConfigData:
  """
  配置文件类
  """

  rds: RDSInfo = None
  redis: RedisInfo = None
  mongodb: MongodbInfo = None
  opensearch: OpenSearchInfo = None

  logger: Log = None
  script_directory_path: str = ""
  mode: str = "normal"
  online_upgrade: str = "true"
  stage: str = ""

  rds_databases: List[str] = None
  mongodb_databases: List[str] = None
  renamed_services: List[dict] = None
