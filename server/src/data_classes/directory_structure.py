#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.utils.util import get_max_version


class File:
  def __init__(self, file_name: str = "", parent_path: str = ""):
    self.file_name = file_name
    self.path = self.get_path(parent_path)

  def get_path(self, parent_path):
    path = os.path.join(parent_path, self.file_name)
    return path


class StageDir:
  def __init__(self, stage="", parent_path: str = ""):
    # pre or post
    self.stage = stage
    self.path = self.get_path(parent_path)
    self.files = self.setup()

  def get_path(self, parent_path):
    path = os.path.join(parent_path, self.stage)
    if os.path.exists(path):
      return path
    else:
      return None

  def get_init_file_path(self) -> str:
    """获取<stage>目录下的init文件"""
    path = os.path.join(self.path, "init.sql")
    return path

  def setup(self) -> list[File]:
    if not self.path:
      return []
    files = os.listdir(self.path)
    file_info_list = []
    for file in files:
      version_dir_info = File(file, self.path)
      file_info_list.append(version_dir_info)
    return file_info_list


class VersionDir:
  def __init__(self, version: str = "", parent_path: str = "", stage: str = ""):
    self.version = version
    self.path = self.get_path(parent_path)
    # self.stage_dir = stage_dir
    self.stage_dir = self.setup(stage)
    self.stage = stage

  def get_init_file_path(self) -> str:
    """获取当前版本下的init文件"""
    init_file_path = os.path.join(self.path, self.stage, "init.sql")
    return init_file_path

  def get_path(self, parent_path: str):
    path = os.path.join(parent_path, self.version)
    return path

  def setup(self, stage) -> StageDir:
    stage_dir = StageDir(stage, self.path)
    return stage_dir


class DatabaseTypeDir:
  def __init__(self, db_type: str = "", parent_path: str = "", stage: str = ""):
    self.path = self.get_path(parent_path, db_type)
    self.stage = stage
    self.version_dir_list = self.setup()

  def get_max_version_info(self) -> VersionDir:
    max_version = get_max_version(os.listdir(self.path))
    for version_dir in self.version_dir_list:
      if version_dir.version == max_version:
        return version_dir

  def get_all_version(self):
    return os.listdir(self.path)

  def get_path(self, parent_path: str, db_type: str):
    """获取当前数据类型目录所在的路径"""
    self.db_type = db_type.lower()
    db_type_dir_path = os.path.join(parent_path, self.db_type)
    if not os.path.exists(db_type_dir_path):
      self.db_type = "mariadb"
      db_type_dir_path = os.path.join(parent_path, self.db_type)
    return db_type_dir_path

  def setup(self) -> list[VersionDir]:
    """实例化数据类型对应的对象"""
    versions = os.listdir(self.path)
    version_dirs = []
    for version in versions:
      version_dir_info = VersionDir(version, self.path, self.stage)
      version_dirs.append(version_dir_info)
    return version_dirs


class ServiceDir:
  def __init__(self, service_name, cfg: ConfigData = None):
    self.service_name = service_name
    self.cfg = cfg
    self.path = self.get_path(self.cfg.script_directory_path)
    self.database_type_dir_info = self.setup()

  def get_path(self, path: str) -> str:
    service_dir_path = os.path.join(path, self.service_name)
    return service_dir_path

  def get_max_version_init_file(self) -> str:
    """获取最大版本的init.sql路径"""
    max_init_file = self.database_type_dir_info.get_max_version_info().get_init_file_path()
    if not os.path.exists(max_init_file):
      raise Exception(f"{self.service_name} service not exist {max_init_file}")
    return max_init_file

  def setup(self) -> DatabaseTypeDir:
    """实例化数据类型对应的对象"""
    db_type_dir_into = DatabaseTypeDir(self.cfg.rds.type, self.path, self.cfg.stage)
    return db_type_dir_into
