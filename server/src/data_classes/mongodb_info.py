#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Dict


@dataclass
class MongodbInfo:
  host: str
  port: int
  user: str
  password: str
  replicaSet: str
  options: Dict[str, str]
  mgmt_host: str
  mgmt_port: int
  admin_key: str
  deployTrait: Dict[str, str] = None
  ssl: str = "false"
  source_type: str = "internal"

  def to_dict(self) -> dict:
    return {
      "host": self.host,
      "port": self.port,
      "user": self.user,
      "password": self.password,
      "replicaSet": self.replicaSet,
      "options": self.options,
      "mgmt_host": self.mgmt_host,
      "mgmt_port": self.mgmt_port,
      "admin_key": self.admin_key,
      "source_type": self.source_type,
    }
