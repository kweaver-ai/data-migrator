#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import subprocess

from utils.util import copy_files


DEFAULT_DB_TYPE = "mariadb"
COMMON_URL = "https://devops.aishu.cn/AISHUDevOps/{}/_git/{}"


def pull_repos(config: dict):
  print("\n开始拉取代码库")

  source_path = os.path.join(os.getcwd(), "source_code")
  os.makedirs(source_path, exist_ok=True)

  # 使用 config 字典中的参数进行操作
  # https://devops.aishu.cn/AISHUDevOps/{}/_git/{}
  pat = os.getenv("PAT")
  base_commands = [
    "set -ex",
    f"MY_PAT={pat}",
    "echo $MY_PAT",
    """B64_PAT=$(printf ":$MY_PAT" | base64)""",
  ]

  services = config["services"]
  for service in services.keys():
    service_info = services.get(service)
    project = service_info["project"]
    repo = service_info["repo"]
    ref = service_info["ref"]

    print(f"\n拉取代码库: {project}/{repo}, 分支: {ref}")

    service_path = os.path.join(os.getcwd(), "source_code", service)
    if os.path.exists(service_path):
      print(f"本地目录已存在: {service_path}")
    else:
      git_url = COMMON_URL.format(project, repo)
      commands = base_commands + [
        "set -ex",
        f"ref={ref}",
        f"git_url={git_url}",
        f"service_path={service_path}",
        """git clone -c http.extraHeader="Authorization: Basic ${B64_PAT}" --depth 1 -b ${ref} ${git_url} ${service_path} """,
      ]
      commands_str = " && ".join(commands)
      # print(f"commands: {commands_str}")

      try:
        custom_env = os.environ.copy()
        print("环境变量：", custom_env)
        result = subprocess.run(commands_str, env=custom_env, capture_output=True, shell=True, text=True, check=True)
        print("命令输出:", result.stdout)
      except subprocess.CalledProcessError as e:
        error_msg = f"命令 '{commands_str}' 执行失败，返回码: {e.returncode}. output: {e.output}, 错误信息: {e.stderr}"
        raise Exception(f"拉取代码仓库失败: {project}/{repo}, {error_msg}")
      except subprocess.TimeoutExpired as e:
        error_msg = f"命令 '{commands_str}' 执行超时. output: {e.output}, 错误信息: {e.stderr}"
        raise Exception(f"拉取代码仓库失败: {project}/{repo}, {error_msg}")
      except Exception as e:
        error_msg = f"执行命令时发生未知错误: {e}"
        raise Exception(f"拉取代码仓库失败: {project}/{repo}, {error_msg}")

  print("\n拉取代码库完成")


def collect_repos(config: dict):
  print("\n开始复制代码库目录")

  db_types = config["db_types"]
  services = config["services"]
  for service in services.keys():
    service_info = services.get(service)
    db_path = service_info["path"]

    print(f"\n复制代码库目录: {service}, {db_path}")

    source_path = os.path.join(os.getcwd(), "source_code", service, db_path)
    repo_path = os.path.join(os.getcwd(), "repos", service)
    os.makedirs(repo_path, exist_ok=True)

    for db_type in db_types:
      source_db_path = os.path.join(source_path, db_type)
      if not os.path.isdir(source_db_path):
        print(f"\n待复制的db_type({db_type})不存在，将会复制默认db_type({DEFAULT_DB_TYPE})")
        source_db_path = os.path.join(source_path, DEFAULT_DB_TYPE)
        if not os.path.isdir(source_db_path):
          raise Exception(f"服务 {service} 缺少目录: {db_type}")

      repo_db_path = os.path.join(repo_path, db_type)
      os.makedirs(repo_db_path, exist_ok=True)

      copy_files(source_db_path, repo_db_path)

  print("\n复制代码库目录完成")
