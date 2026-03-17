#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import List

from dataModelManagement.src.data_classes.config_data import ConfigData
from dataModelManagement.src.data_classes.rds_info import RDSInfo
from dataModelManagement.src.db.schema_upgrade_table_db import SchemaUpgradeTableDB
from dataModelManagement.src.db.service_schema import ServiceSchema
from dataModelManagement.src.db.verification_schema_records_db import VerificationSchemaRecords
from dataModelManagement.src.schema_class.column_class import ColumnClass
from dataModelManagement.src.schema_class.constraint_class import ConstraintClass
from dataModelManagement.src.schema_class.key_class import KeyClass
from dataModelManagement.src.schema_class.micro_service import MicroServiceClass
from dataModelManagement.src.schema_class.status_enum import StateEnum
from dataModelManagement.src.schema_class.table_class import TableClass
from dataModelManagement.src.schema_class.unique_class import UniqueClass
from dataModelManagement.src.utils.sql_util import SqlUtil
from dataModelManagement.src.utils.util import get_min_version, get_target_file_schema


class VerificationExecutor:
  def __init__(self, cfg: ConfigData):
    self.cfg = cfg
    self.logger = self.cfg.logger
    self.service_schema_diff_list: List[MicroServiceClass] = []

  def run(self):
    services = self.get_services()
    asyncio.run(self.init_compare_task(services))
    current_timestamp = int(time.time())
    self.insert_verification_result_to_database(current_timestamp)

  def get_services(self) -> list:
    dir_list = os.listdir(self.cfg.script_directory_path)
    services = [dir_name for dir_name in dir_list]
    return services

  async def init_compare_task(self, services: List[str]):
    """
    任务启动器
    :return:
    """
    executor = ThreadPoolExecutor(2)
    loop = asyncio.get_event_loop()
    asyncio.get_event_loop()
    tt = [loop.run_in_executor(executor, self.task, service) for service in services]
    await asyncio.wait(tt)
    return self.service_schema_diff_list

  def task(self, service: str):
    """
    单个任务
    """
    self.logger.info(f"start {service} service data schema verify...")
    executor = ActionTask(self.cfg, self.logger, service)
    sign = executor.compare_schema()
    if sign == 1:
      self.service_schema_diff_list.append(executor.micro_service_info)
    else:
      pass

  def insert_verification_result_to_database(self, verify_end_time: int):
    verification_schema_records = VerificationSchemaRecords(self.cfg.rds, self.logger)
    result = "pass"
    test_entries = []
    # 与应用实例对应
    ai_id = os.environ.get("AIID")
    for micro_service_result in self.service_schema_diff_list:
      # 单个服务差异
      service_dict = {}
      if len(micro_service_result.add_tables) == len(micro_service_result.miss_tables) == len(micro_service_result.diff_tables) == 0:
        service_dict["test_result"] = "pass"
        service_dict["test_result_details"] = ""
        service_dict["service_name"] = micro_service_result.service_name
      elif len(micro_service_result.miss_tables) > 0:
        self.logger.info("result is fail")
        result = "fail"
        json_str = micro_service_result.get_result()
        service_dict["test_result"] = "fail"
        service_dict["test_result_details"] = json_str
        service_dict["service_name"] = micro_service_result.service_name
      else:
        json_str = micro_service_result.get_result()
        for table in micro_service_result.diff_tables:
          self.logger.info(f"table name : {table.table_name}")
          self.logger.info(f"table sign : {table.get_sign()}")
          if table.get_sign() >= 8:
            self.logger.info("result is fail > 8")
            result = "fail"
            service_dict["test_result"] = "fail"
            service_dict["test_result_details"] = json_str
            service_dict["service_name"] = micro_service_result.service_name
          else:
            self.logger.info("result is warn")
            service_dict["test_result"] = "warn"
            service_dict["test_result_details"] = json_str
            service_dict["service_name"] = micro_service_result.service_name
      test_entries.append(service_dict)
    r_id = verification_schema_records.insert(ai_id=int(ai_id), verify_result=result, verify_end_time=verify_end_time, test_entries=test_entries)
    return r_id


class ActionTask:
  """
  比较单个服务的数据差异
  """

  def __init__(self, cfg: ConfigData, logger: Logger, service: str):
    self.rds = cfg.rds
    self.cfg = cfg
    self.logger = logger
    self.target_tables = {}
    self.current_tables = {}
    self.exist_flag = False
    self.sql_util = SqlUtil(self.logger)
    service_version = SchemaUpgradeTableDB(self.rds, self.logger).get_micro_service_installed_version(service)
    if service_version:
      self.exist_flag = True
      if service_version == "1.0.0":
        version_list = os.listdir(os.path.join(self.cfg.script_directory_path, service, "mariadb"))
        service_version = get_min_version(version_list)
      self.micro_service_info = MicroServiceClass(
        service_name=service, add_tables=[], miss_tables=[], diff_tables=[], service_version=service_version
      )
      self.target_init_file_path = os.path.join(self.cfg.script_directory_path, service, "mariadb", service_version, "pre", "init.sql")

  def compare_schema(self) -> int:
    """
    比较当前环境中的数据库结构和期望数据库结构，并具体返回差异已体现在哪里
    :return:
    """
    if not self.exist_flag:
      return 0
    self._get_target_mariadb_schema(self.target_init_file_path)
    print(self.target_init_file_path)
    self._get_current_mariadb_schema()
    diff_object = {}
    diff_object["tables"] = {}
    # 以下四个字段为未来按需扩展
    diff_object["servers"] = {}
    diff_object["events"] = {}
    diff_object["routines"] = {}
    diff_object["triggers"] = {}

    if self.target_tables or self.current_tables:
      self.logger.info(f"Verifying {self.micro_service_info.service_name} data model")

      for table in self.target_tables:
        if table in self.current_tables.keys():
          target_table_value = self.target_tables[table].replace("\n", "").replace("'", "").replace(";", "").replace(" ", "").replace("`", "").lower()
          current_table_value = (
            self.current_tables[table]["Create Table"].replace("\n", "").replace("'", "").replace(" ", "").replace(";", "").replace("`", "").lower()
          )
          if target_table_value == current_table_value:
            pass
          else:
            # 两个表的整体定义，只装载不处理
            diff_object = self._get_diff_schema_for_table(
              tables_name=table,
              target_table_schema=self.target_tables[table],
              current_table_schema=self.current_tables[table]["Create Table"],
              diff_object=diff_object,
            )
        else:
          # 缺失表
          self.logger.info(f"Verified missing {table} table")
          self.micro_service_info.miss_tables.append(TableClass(table_name=table))
      # 数量一致，则表没有新增
      if len(self.target_tables) == len(self.current_tables):
        pass
      else:
        # 计算新增表
        result_table = set(self.current_tables.keys()) - set(self.target_tables.keys())
        # TODO多出的表警告
        for table in list(result_table):
          self.micro_service_info.add_tables.append(TableClass(table_name=table))

      # 对所有装载的schema比较进行逻辑处理，加入到micro_service_info中
      self._compare_definitions(diff_object["tables"])
      return 1

    # 两者皆为空，数据库一致
    else:
      self.logger.info("Database validation results: consistent schema")
      return 0

  def _get_diff_schema_for_table(self, tables_name: str, target_table_schema: str, current_table_schema: str, diff_object: dict) -> dict:
    diff_object["tables"][tables_name] = {}
    current_table_schema = current_table_schema + ";"
    target_definitions = self._get_table_definitions(target_table_schema)
    diff_object["tables"][tables_name]["target_definitions"] = target_definitions
    current_definitions = self._get_table_definitions(current_table_schema)
    diff_object["tables"][tables_name]["current_definitions"] = current_definitions

    return diff_object

  def _compare_definitions(self, schema_tables):
    for table in schema_tables:
      target_table = schema_tables[table]["target_definitions"]
      current_table = schema_tables[table]["current_definitions"]
      table_class_info = TableClass(table_name=table)
      table_class_info = self._column(target_table, current_table, table_class_info)
      table_class_info = self._primary(target_table, current_table, table_class_info)
      table_class_info = self._unique(target_table, current_table, table_class_info)
      table_class_info = self._key(target_table, current_table, table_class_info)
      # 适配多种不同数据库时，constraint有较大差异。之后重新考虑适配方式
      # table_class_info = self._constraint(target_table, current_table, table_class_info)
      if table_class_info.sign != 0:
        self.micro_service_info.diff_tables.append(table_class_info)

  def _constraint(self, target_table_schema, current_table_schema, table_class_info):
    target_constraint = target_table_schema["constraint"]
    current_constraint = current_table_schema["constraint"]
    # 标识constraint是否有差异
    sign = 0
    for definition in target_constraint:
      if definition in current_constraint.keys():
        pass
      else:
        # 缺失字段处理
        constraint_class_info = ConstraintClass(definition, StateEnum.STATE_MISSING)
        constraint_class_info.constraint_target = target_constraint[definition]
        table_class_info.constraints.append(constraint_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1

    for definition in current_constraint:
      if definition in target_constraint.keys():
        target_constraint_definition = target_constraint[definition].replace("'", "").replace(" ", "").lower()
        current_constraint_definition = current_constraint[definition].replace("'", "").replace(" ", "").lower()
        if target_constraint_definition == current_constraint_definition:
          # 没变化跳过
          pass
        else:
          constraint_class_info = ConstraintClass(definition, StateEnum.STATE_CHANGED)
          constraint_class_info.constraint_current = current_constraint[definition]
          constraint_class_info.constraint_target = target_constraint[definition]
          table_class_info.constraints.append(constraint_class_info)
          # table_class_info.sign = table_class_info.sign + 1
          sign = sign + 1
      else:
        # 多出字段处理
        constraint_class_info = ConstraintClass(definition, StateEnum.STATE_ADDED)
        constraint_class_info.constraint_current = current_constraint[definition]
        table_class_info.constraints.append(constraint_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1
    if sign != 0:
      table_class_info.sign = table_class_info.sign + 1
    return table_class_info

  def _key(self, target_table_schema, current_table_schema, table_class_info):
    target_key = target_table_schema["key"]
    current_key = current_table_schema["key"]
    # 标识key是否有差异
    sign = 0
    for definition in target_key:
      if definition in current_key.keys():
        pass
      else:
        # 缺失字段处理
        key_class_info = KeyClass(definition, StateEnum.STATE_MISSING)
        key_class_info.key_target = target_key[definition]
        table_class_info.keys.append(key_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1

    for definition in current_key:
      if definition in target_key.keys():
        target_key_definition = target_key[definition].replace("'", "").replace(" ", "").lower()
        current_key_definition = current_key[definition].replace("'", "").replace(" ", "").lower()
        if target_key_definition == current_key_definition:
          # 没变化跳过
          pass
        else:
          key_class_info = KeyClass(definition, StateEnum.STATE_CHANGED)
          key_class_info.key_current = current_key[definition]
          key_class_info.key_target = target_key[definition]
          table_class_info.keys.append(key_class_info)
          # table_class_info.sign = table_class_info.sign + 1
          sign = sign + 1
      else:
        # 多出字段处理
        key_class_info = KeyClass(definition, StateEnum.STATE_ADDED)
        key_class_info.key_current = current_key[definition]
        table_class_info.keys.append(key_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1
    if sign != 0:
      table_class_info.sign = table_class_info.sign + 2
    return table_class_info

  def _unique(self, target_table_schema, current_table_schema, table_class_info):
    target_unique = target_table_schema["unique"]
    current_unique = current_table_schema["unique"]
    # 标识unique是否有差异
    sign = 0
    for definition in target_unique:
      if definition in current_unique.keys():
        if (
          target_unique[definition].replace("'", "").replace(" ", "").replace(",", "").replace("`", "").lower()
          == current_unique[definition].replace("'", "").replace(" ", "").replace(",", "").replace("`", "").lower()
        ):
          pass
        else:
          # 唯一约束有差异处理
          unique_class_info = UniqueClass(definition, StateEnum.STATE_CHANGED)
          unique_class_info.unique_current = current_unique[definition]
          unique_class_info.unique_target = target_unique[definition]
          table_class_info.uniques.append(unique_class_info)
          # table_class_info.sign = table_class_info.sign + 1
          sign = sign + 1
      else:
        # 原表没有主键，对照表有
        unique_class_info = UniqueClass(definition, StateEnum.STATE_CHANGED)
        unique_class_info.unique_target = target_unique[definition]
        table_class_info.uniques.append(unique_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1

    for definition in current_unique:
      if definition in target_unique.keys():
        pass
      else:
        # 原表有，对照表没有
        unique_class_info = UniqueClass(definition, StateEnum.STATE_ADDED)
        unique_class_info.unique_current = current_unique[definition]
        table_class_info.uniques.append(unique_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1
    if sign != 0:
      table_class_info.sign = table_class_info.sign + 4
    return table_class_info

  def _primary(self, target_table_schema, current_table_schema, table_class_info: TableClass):
    target_primary = target_table_schema["primary"]
    current_primary = current_table_schema["primary"]
    # 标识primary是否有差异
    if "primary" in target_primary.keys():
      if "primary" in current_primary.keys():
        if (
          target_primary["primary"].replace("'", "").replace(" ", "").lower() == current_primary["primary"].replace("'", "").replace(" ", "").lower()
        ):
          pass
        else:
          # 主键有差异处理
          table_class_info.target_primary = target_primary["primary"]
          table_class_info.current_primary = current_primary["primary"]
          table_class_info.primary_status = 3
          # table_class_info.sign = table_class_info.sign + 1
          table_class_info.sign = table_class_info.sign + 8
          return table_class_info
      else:
        # 原表没有主键，对照表有
        table_class_info.target_primary = target_primary["primary"]
        table_class_info.primary_status = 2
        # table_class_info.sign = table_class_info.sign + 1
        table_class_info.sign = table_class_info.sign + 8
        return table_class_info

    if "primary" in current_primary.keys():
      if "primary" in target_primary.keys():
        pass
      else:
        # 原表含有主键，对照表没有
        table_class_info.current_primary = current_primary["primary"]
        table_class_info.primary_status = 1
        # table_class_info.sign = table_class_info.sign + 1
        table_class_info.sign = table_class_info.sign + 8
        return table_class_info
    return table_class_info

  def _column(self, target_table_schema, current_table_schema, table_class_info: TableClass):
    target_column = target_table_schema["column"]
    current_column = current_table_schema["column"]
    sign = 0
    for definition in target_column:
      if definition in current_column.keys():
        pass
      else:
        # 缺失字段处理
        column_class_info = ColumnClass(definition, StateEnum.STATE_MISSING)
        column_class_info.column_target = target_column[definition]
        table_class_info.columns.append(column_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1

    for definition in current_column:
      if definition in target_column.keys():
        # 针对mysql和mariadb的json进行处理

        if "longtext" in target_column[definition].split() and "json" in current_column[definition].split():
          pass
        else:
          # 由于公有云，TIDB字符编码不一，忽略字符编码比较
          target_column_definition = (
            target_column[definition]
            .replace("'", "")
            .replace("COLLATE", "")
            .replace(" ", "")
            .replace("utf8_bin", "")
            .replace("utf8mb4_bin", "")
            .replace("utf8mb4_unicode_ci", "")
            .replace("(", "")
            .replace(")", "")
            .lower()
          )
          current_column_definition = (
            current_column[definition]
            .replace("'", "")
            .replace(" ", "")
            .replace("COLLATE", "")
            .replace("utf8mb4_bin", "")
            .replace("utf8_bin", "")
            .replace("utf8mb4_unicode_ci", "")
            .replace("utf8_general_ci", "")
            .replace("(", "")
            .replace(")", "")
            .lower()
          )
          if target_column_definition == current_column_definition:
            # 没变化跳过
            pass
          else:
            # 适配云环境默认值为null不显示
            if (
              target_column_definition.replace("defaultnull", "") == current_column_definition
              or target_column_definition.replace("default", "") == current_column_definition
            ):
              pass
            else:
              column_class_info = ColumnClass(definition, StateEnum.STATE_CHANGED)
              column_class_info.column_current = current_column[definition]
              column_class_info.column_target = target_column[definition]
              table_class_info.columns.append(column_class_info)
              # table_class_info.sign = table_class_info.sign + 1
              sign = sign + 1
      else:
        # 多出字段处理
        column_class_info = ColumnClass(definition, StateEnum.STATE_ADDED)
        column_class_info.column_current = current_column[definition]
        table_class_info.columns.append(column_class_info)
        # table_class_info.sign = table_class_info.sign + 1
        sign = sign + 1
    if sign != 0:
      table_class_info.sign = table_class_info.sign + 16
    return table_class_info

  def _get_table_definitions(self, schema_table: str) -> dict:
    """
    解析一个create语句的方法
    """
    return_definitions = {}
    return_definitions["column"] = {}
    return_definitions["primary"] = {}
    return_definitions["unique"] = {}
    return_definitions["key"] = {}
    return_definitions["constraint"] = {}
    return_definitions["option"] = {}
    table_definitions = self.sql_util.sql_parse(schema_table)
    for table_definition in table_definitions:
      column_name = str(table_definition[0]).lower()
      if column_name == "primary" or column_name == "unique" or column_name == "key" or column_name == "constraint" or column_name == "index":
        pass
      else:
        definition = " ".join(str(t) for t in table_definition[1:])
        column_name = column_name.replace("`", "")
        if definition:
          return_definitions["column"][column_name] = column_name
          if "primarykey" in definition.replace(" ", "").lower():
            return_definitions["primary"]["primary"] = "(" + column_name.replace("`", "").lower() + ")"

    table_definitions = schema_table.split("\n")
    for definition in table_definitions:
      primary_name = re.match(r"(\s*PRIMARY KEY\s*)", definition, re.IGNORECASE)
      if primary_name:
        return_definitions["primary"]["primary"] = (
          re.match(r"(\s*)(PRIMARY KEY)(.*?)(\(.*\))(,?)", definition, re.IGNORECASE).group(4).replace("`", "").lower()
        )

      unique_name = re.match(r"\s*(?:UNIQUE KEY|UNIQUE).*?(\(.*\))(,?)", definition, re.IGNORECASE)
      if unique_name:
        index_l = definition.find("(")
        index_r = definition.rfind(")")
        key = definition[index_l : index_r + 1]
        return_definitions["unique"][key.replace("`", "").replace(" ", "").lower()] = key.replace("`", "").replace(" ", "").lower()

      key_name = re.match(r"(\s*)(KEY.*\))(,?)", definition, re.IGNORECASE)
      index_name = re.match(r"(\s*)(INDEX.*\))(,?)", definition, re.IGNORECASE)
      if key_name:
        index_l = key_name.group(2).find("(")
        index_r = key_name.group(2).rfind(")")
        key = key_name.group(2)[index_l : index_r + 1]
        return_definitions["key"][key.replace("`", "").replace(" ", "").lower()] = key.replace("`", "").replace(" ", "").lower()
      elif index_name:
        index_l = index_name.group(2).find("(")
        index_r = index_name.group(2).rfind(")")
        key = index_name.group(2)[index_l : index_r + 1]
        return_definitions["key"][key.replace("`", "").replace(" ", "").lower()] = key.replace("`", "").replace(" ", "").lower()

      # 约束语法，不同类型数据库差距过大，暂时不考虑加入
      # constraint_name = re.match(r"(\s*)(CONSTRAINT[^,]*)(,?)", definition, re.IGNORECASE)
      # if constraint_name:
      #     return_definitions['constraint'][constraint_name.group(2).replace("`", "").replace(" ", '').lower()]\
      #         = re.match(r"(\s*)(CONSTRAINT[^,]*)(,?)", definition).group(2).replace("`", "").replace(" ", '').lower()
      #
      # option_name = re.match(r"(\)\s*ENGINE=.*)", definition)
      # if option_name:
      #     pattern = re.compile(r' AUTO_INCREMENT=\d+| ROW_FORMAT=\w+', re.I)
      #     engine_content = re.sub(pattern, '', re.match(r"(\)\s*)(ENGINE[^\n]*)(;?)", definition).group(2).replace("`", ""))
      #     return_definitions['option'][kc_file_comment_like'option'] = engine_content
    return return_definitions

  def _get_current_mariadb_schema(self):
    """
    获取当前服务所有的表
    :param :
    :return:
    """
    self.current_tables = ServiceSchema(self.rds, self.logger).get_table_schema(self.target_tables)

  def _get_target_mariadb_schema(self, path: str):
    """
    解析指定路径的init文件
    """
    try:
      #
      res_sql_list = get_target_file_schema(path)
    except Exception as ex:
      return ex
    current_database = None

    for sql in res_sql_list:
      # 匹配数据库
      use_pattern = re.compile(r"^USE\s+(.+)", re.IGNORECASE)
      database_match = use_pattern.search(sql)
      if database_match:
        current_database = database_match.group(1).replace(" ", "").replace("`", "")
        current_database = self.rds.system_id + current_database
        continue
      else:
        create_table_pattern = re.compile(r"(CREATE\s*TABLE\s*IF\s*NOT\s*EXISTS)([^\(]*)(\()")

        table_matches = create_table_pattern.search(self.sql_util.format_sql(sql))
        if table_matches:
          table_key = table_matches.group(2).replace("`", "")
          if "." not in table_key and current_database:
            table_key = f"`{current_database}`.`{table_key}`".replace(" ", "")
            self.target_tables[table_key] = sql
          elif "." in table_key:
            self.target_tables[table_key.replace(" ", "")] = sql
          else:
            print("The expected data model file structure is incorrect. Please contact relevant personnel to check")
            sys.exit(1)
