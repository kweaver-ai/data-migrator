#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
from dataclasses import dataclass
from typing import Dict


@dataclass
class SchemaUpgrade(object):
  service_name: str
  script_file_name: str
  installed_version: str
  target_version: str
  status: str
  create_time: datetime
  update_time: datetime

  def to_dict(self) -> Dict:
    return {
      "service_name": self.service_name,
      "script_file_name": self.script_file_name,
      "installed_version": self.installed_version,
      "target_version": self.target_version,
      "status": self.status,
      "create_time": self.create_time,
      "update_time": self.update_time,
    }
