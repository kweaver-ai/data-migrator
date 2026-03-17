#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Dict


@dataclass
class RDSInfo(object):
  host: str
  port: int
  user: str
  password: str
  type: str
  hostRead: str
  portRead: int
  source_type: str
  admin_key: str
  mgmt_host: str
  mgmt_port: int
  system_id: str = ""
  deployTrait: Dict[str, str] = None

  def to_dict(self):
    return {
      "host": self.host,
      "port": self.port,
      "user": self.user,
      "password": self.password,
      "db_type": self.type,
      "source_type": self.source_type,
      "admin_key": self.admin_key,
      "mgmt_host": self.mgmt_host,
      "mgmt_port": self.mgmt_port,
      "system_id": self.system_id,
    }

  def get_deploy_db_name(self):
    # 获取deploy数据库名称
    return self.system_id + "deploy"
