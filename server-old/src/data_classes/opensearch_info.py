#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Dict


@dataclass
class OpenSearchInfo:
  host: str
  port: int
  user: str
  password: str
  protocol: str
  distribution: str = ""
  version: str = ""
  deployTrait: Dict[str, str] = None

  def to_dict(self) -> dict:
    return {
      "host": self.host,
      "port": self.port,
      "user": self.user,
      "password": self.password,
      "protocol": self.protocol,
    }
