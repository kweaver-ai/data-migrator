#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""RDSConfig source_type 校验 + loader 默认值测试"""
import textwrap
import tempfile
import os
import logging
import pytest

from server.config.models import RDSConfig
from server.config.loader import load_config


def make_rds(**kwargs) -> RDSConfig:
    defaults = dict(host="h", port=3306, user="u", password="p", type="mariadb")
    defaults.update(kwargs)
    return RDSConfig(**defaults)


class TestRDSConfigSourceTypeValidation:
    def test_internal_is_valid(self):
        rds = make_rds(source_type="internal")
        assert rds.source_type == "internal"

    def test_external_is_valid(self):
        rds = make_rds(source_type="external")
        assert rds.source_type == "external"

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="source_type 非法值"):
            make_rds(source_type="unknown")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="source_type 非法值"):
            make_rds(source_type="")

    def test_case_sensitive_raises(self):
        with pytest.raises(ValueError, match="source_type 非法值"):
            make_rds(source_type="Internal")


class TestLoaderSourceTypeDefault:
    def _write_yaml(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        f.write(textwrap.dedent(content))
        f.close()
        return f.name

    def test_source_type_defaults_to_internal(self):
        path = self._write_yaml("""
            rds:
              host: localhost
              port: 3306
              user: root
              password: pass
              type: mariadb
        """)
        try:
            cfg = load_config(path, None, logging.getLogger("test"))
            assert cfg.rds.source_type == "internal"
        finally:
            os.unlink(path)

    def test_source_type_external_loaded(self):
        path = self._write_yaml("""
            rds:
              host: localhost
              port: 3306
              user: root
              password: pass
              type: mariadb
              source_type: external
        """)
        try:
            cfg = load_config(path, None, logging.getLogger("test"))
            assert cfg.rds.source_type == "external"
        finally:
            os.unlink(path)

    def test_invalid_source_type_in_yaml_raises(self):
        path = self._write_yaml("""
            rds:
              host: localhost
              port: 3306
              user: root
              password: pass
              type: mariadb
              source_type: typo
        """)
        try:
            with pytest.raises(ValueError, match="source_type 非法值"):
                load_config(path, None, logging.getLogger("test"))
        finally:
            os.unlink(path)
