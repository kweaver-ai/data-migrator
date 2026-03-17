#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
from typing import Dict


@dataclass
class RedisConnectInfo:
  username: str = ""
  password: str = ""
  host: str = ""
  port: int = 0
  masterHost: str = ""
  masterPort: int = 0
  slaveHost: str = ""
  slavePort: int = 0
  sentinelHost: str = ""
  sentinelPort: int = 0
  sentinelUsername: str = ""
  sentinelPassword: str = ""
  masterGroupName: str = ""
  deployTrait: Dict[str, str] = None

  def to_dict(self):
    return asdict(self)


@dataclass
class RedisInfo:
  connectInfo: RedisConnectInfo
  connectType: str = ""

  def to_dict(self):
    return {"connectInfo": self.connectInfo.to_dict(), "connectType": self.connectType}
