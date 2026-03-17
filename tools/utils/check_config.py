#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class CheckConfig:
  CheckLatest = 1
  CheckRecently = 2
  CheckAll = 3

  CheckType = CheckRecently

  AllowNonePrimaryKey = False
  AllowForeignKey = False
  AllowPythonException = False
  AllowTableCompareDismatch = True

  def __init__(self, config: dict):
    self.DBTypes = [db_type.lower() for db_type in config.get("db_types", [])]
    self.DATABASES = [db.lower() for db in config.get("databases", [])]

    check_rules = config.get("check_rules", {})
    self.CheckType = check_rules.get("check_type", CheckConfig.CheckLatest)
    self.AllowNonePrimaryKey = check_rules.get("allow_none_primary_key", CheckConfig.AllowNonePrimaryKey)
    self.AllowForeignKey = check_rules.get("allow_foreign_key", CheckConfig.AllowForeignKey)
    self.AllowPythonException = check_rules.get("allow_python_exception", CheckConfig.AllowPythonException)
    self.AllowTableCompareDismatch = check_rules.get("allow_table_compare_dismatch", CheckConfig.AllowTableCompareDismatch)
