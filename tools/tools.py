#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import yaml

import collect_repo
import check_repo
import check_schema


if __name__ == "__main__":
  # 读取传递的模板清单文件
  config_file = sys.argv[1]
  with open(config_file, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

  # 拉取所有部署服务的仓库
  collect_repo.pull_repos(config)

  # 收集目标类型目录
  collect_repo.collect_repos(config)

  # 检查目录
  check_repo.check_repos(config)

  # 运行sql代码
  check_schema.check_repos(config)
