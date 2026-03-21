#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright The kweaver.ai Authors.
#
# Licensed under the Apache License, Version 2.0.
# See the LICENSE file in the project root for details.
"""HistoryManager 单元测试

mock self.db（OperateDB）和 sha256_file，验证 record/check_checksum 行为。
"""
from unittest.mock import MagicMock, patch

import pytest

from server.migrate.history_manager import HistoryManager
from server.migrate.task_manager import TaskStatus


def make_manager():
    mgr = HistoryManager.__new__(HistoryManager)
    mgr.db = MagicMock()
    mgr.deploy_db = "deploy"
    mgr.logger = MagicMock()
    return mgr


class TestGetCreateTableSql:
    def test_uses_table_class_attribute(self):
        sql = HistoryManager.get_create_table_sql("mydb")
        assert HistoryManager.TABLE in sql

    def test_uses_deploy_db_prefix(self):
        sql = HistoryManager.get_create_table_sql("mydb")
        assert "mydb." in sql

    def test_contains_required_columns(self):
        sql = HistoryManager.get_create_table_sql("deploy")
        for col in ["f_id", "f_service_name", "f_version",
                    "f_script_file_name", "f_checksum", "f_status", "f_create_time"]:
            assert col in sql, f"缺少列: {col}"

    def test_has_create_if_not_exists(self):
        sql = HistoryManager.get_create_table_sql("deploy")
        assert "CREATE TABLE IF NOT EXISTS" in sql


class TestRecord:
    def test_inserts_with_all_required_fields(self):
        mgr = make_manager()
        with patch("server.migrate.history_manager.sha256_file", return_value="abc123"):
            mgr.record("svc", "1.1.0", "1.1.0/01-a.sql", "/path/01-a.sql", TaskStatus.SUCCESS)

        _, data = mgr.db.insert.call_args[0]
        assert data["f_service_name"] == "svc"
        assert data["f_version"] == "1.1.0"
        assert data["f_script_file_name"] == "1.1.0/01-a.sql"
        assert data["f_checksum"] == "abc123"
        assert "f_create_time" in data

    def test_status_success_stored(self):
        mgr = make_manager()
        with patch("server.migrate.history_manager.sha256_file", return_value=""):
            mgr.record("svc", "1.0.0", "1.0.0/init.sql", "/path", TaskStatus.SUCCESS)

        _, data = mgr.db.insert.call_args[0]
        assert data["f_status"] == TaskStatus.SUCCESS

    def test_status_failed_stored(self):
        mgr = make_manager()
        with patch("server.migrate.history_manager.sha256_file", return_value=""):
            mgr.record("svc", "1.0.0", "1.0.0/01-a.sql", "/path", TaskStatus.FAILED)

        _, data = mgr.db.insert.call_args[0]
        assert data["f_status"] == TaskStatus.FAILED

    def test_empty_script_path_gives_empty_checksum(self):
        mgr = make_manager()
        mgr.record("svc", "1.0.0", "1.0.0/init.sql", "", TaskStatus.FAILED)

        _, data = mgr.db.insert.call_args[0]
        assert data["f_checksum"] == ""

    def test_checksum_computed_from_path(self):
        mgr = make_manager()
        with patch("server.migrate.history_manager.sha256_file", return_value="deadbeef") as mock_sha:
            mgr.record("svc", "1.0.0", "1.0.0/01.sql", "/real/path.sql", TaskStatus.SUCCESS)

        mock_sha.assert_called_once_with("/real/path.sql")
        _, data = mgr.db.insert.call_args[0]
        assert data["f_checksum"] == "deadbeef"

    def test_targets_correct_table(self):
        mgr = make_manager()
        with patch("server.migrate.history_manager.sha256_file", return_value=""):
            mgr.record("svc", "1.0.0", "1.0.0/init.sql", "", TaskStatus.SUCCESS)

        table_arg = mgr.db.insert.call_args[0][0]
        assert "deploy" in table_arg
        assert HistoryManager.TABLE in table_arg


class TestCheckChecksum:
    def test_no_prior_record_returns_true(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = None

        result = mgr.check_checksum("svc", "1.0.0", "1.0.0/01.sql", "/path.sql")

        assert result is True

    def test_empty_stored_checksum_returns_true(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = {"f_checksum": ""}

        with patch("server.migrate.history_manager.sha256_file", return_value="abc"):
            result = mgr.check_checksum("svc", "1.0.0", "1.0.0/01.sql", "/path.sql")

        assert result is True

    def test_matching_checksum_returns_true(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = {"f_checksum": "abc123"}

        with patch("server.migrate.history_manager.sha256_file", return_value="abc123"):
            result = mgr.check_checksum("svc", "1.0.0", "1.0.0/01.sql", "/path.sql")

        assert result is True

    def test_mismatched_checksum_returns_false(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = {"f_checksum": "original"}

        with patch("server.migrate.history_manager.sha256_file", return_value="tampered"):
            result = mgr.check_checksum("svc", "1.0.0", "1.0.0/01.sql", "/path.sql")

        assert result is False

    def test_mismatch_logs_warning(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = {"f_checksum": "original"}

        with patch("server.migrate.history_manager.sha256_file", return_value="tampered"):
            mgr.check_checksum("svc", "1.0.0", "1.0.0/01.sql", "/path.sql")

        mgr.logger.warning.assert_called_once()
        warning_msg = mgr.logger.warning.call_args[0][0]
        assert "1.0.0/01.sql" in warning_msg

    def test_queries_by_service_version_and_script(self):
        mgr = make_manager()
        mgr.db.fetch_one.return_value = None

        mgr.check_checksum("svc", "1.1.0", "1.1.0/02.sql", "/path.sql")

        sql, svc, ver, script = mgr.db.fetch_one.call_args[0]
        assert svc == "svc"
        assert ver == "1.1.0"
        assert script == "1.1.0/02.sql"
