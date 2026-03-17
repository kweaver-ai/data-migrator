#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@Time    : 2023/4/23 8:35
@Author  : mario.jiang
@File    : log_util.py
"""

import re
import sys
import logging
from logging import Logger


class PasswordFilter(logging.Filter):
  """
  屏蔽密码
  """

  def filter(self, record: logging.LogRecord) -> bool:
    record.msg = re.sub(
      r'("|\')(password|sentinelPassword)("|\'):([ ]*)("|\')([^"\']*)("|\')',
      r"\1\2\3:\4\5*****\7",
      record.getMessage(),
      flags=re.I,
    )

    return True


class LogDiy:
  def get_logger(self, log_level: str = "INFO") -> Logger:
    _nameToLevel = {
      "CRITICAL": logging.CRITICAL,
      "FATAL": logging.FATAL,
      "ERROR": logging.ERROR,
      "WARN": logging.WARNING,
      "WARNING": logging.WARNING,
      "INFO": logging.INFO,
      "DEBUG": logging.DEBUG,
      "NOTSET": logging.NOTSET,
    }
    log_level = _nameToLevel.get(log_level.upper())
    logger = logging.Logger("DataModerManagement")
    # 必须设置，这里如果不显示设置，默认过滤掉warning之前的所有级别的信息
    logger.setLevel(log_level)

    # stdout日志输出格式
    stdout_formatter = logging.Formatter("[%(asctime)s] %(filename)s %(funcName)s line:%(lineno)d [%(levelname)s] %(message)s")

    # 创建一个FileHandler， 向文件输出日志信息
    # 创建一个StreamHandler， 向stdout输出日志信息
    stdout_handler = logging.StreamHandler(sys.stdout)

    # 设置日志等级
    stdout_handler.setLevel(log_level)
    # 设置handler的格式对象
    stdout_handler.setFormatter(stdout_formatter)
    # 将handler增加到logger中
    logger.addHandler(stdout_handler)
    logger.addFilter(PasswordFilter())
    return logger

  _instance = None

  @classmethod
  def instance(cls) -> "LogDiy":
    if cls._instance is None:
      cls._instance = cls()
    return cls._instance


def log_response(response):
  """
  记录log日志debug级别
  :param response:
  :return:
  """
  log_msg = "Request URL: {url}, method: {method}, code: {status_code}, content: {response_content}".format(
    url=response.request.url,
    method=response.request.method,
    status_code=response.status_code,
    response_content=response.text,
  )
  LogDiy.instance().get_logger().info(log_msg)


def log_response_info(response):
  """
  记录log日志info级别
  :param response:
  :return:
  """
  log_msg = "Request URL: {url}, method: {method}, code: {status_code}, content: {response_content}".format(
    url=response.request.url,
    method=response.request.method,
    status_code=response.status_code,
    response_content=response.text,
  )
  LogDiy.instance().get_logger().info(log_msg)
