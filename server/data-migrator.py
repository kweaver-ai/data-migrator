#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data Migrator - 统一入口
支持三个子命令: migrate, collect, check
"""
import argparse
import sys
import os

# 将 server/ 所在目录加入 sys.path，以支持 from server.xxx import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="data-migrator",
        description="数据库迁移引擎：迁移 / 收集 / 校验",
    )
    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # ── migrate ──
    migrate_parser = subparsers.add_parser("migrate", help="执行数据库初始化和升级迁移")
    migrate_parser.add_argument("--config", required=True, help="YAML 配置文件路径")
    migrate_parser.add_argument("--service", nargs="*", default=None, help="指定本次迁移的服务（默认全部）")
    migrate_parser.add_argument("--log-level", default="INFO", help="日志级别")

    # ── collect ──
    collect_parser = subparsers.add_parser("collect", help="从 Git 仓库拉取并收集迁移脚本")
    collect_parser.add_argument("--config", required=True, help="YAML 配置文件路径")
    collect_parser.add_argument("--service", nargs="*", default=None, help="指定本次收集的服务（默认全部）")
    collect_parser.add_argument("--log-level", default="INFO", help="日志级别")

    # ── check ──
    check_parser = subparsers.add_parser("check", help="校验迁移脚本目录结构和 SQL 语法")
    check_parser.add_argument("--config", required=True, help="YAML 配置文件路径")
    check_parser.add_argument("--service", nargs="*", default=None, help="指定本次校验的服务（默认全部）")
    check_parser.add_argument("--log-level", default="INFO", help="日志级别")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    from server.utils.log import LogDiy
    logger = LogDiy.instance().get_logger(args.log_level)

    if args.command == "migrate":
        from server.config.loader import load_config
        app_config = load_config(args.config, args.service, logger)

        from server.migrate.executor import MigrationExecutor
        executor = MigrationExecutor(app_config, logger)
        executor.run()

    elif args.command == "collect":
        from server.config.loader import load_config
        app_config = load_config(args.config, args.service, logger)

        from server.collect.collector import CollectExecutor
        executor = CollectExecutor(app_config, logger)
        executor.run()

    elif args.command == "check":
        from server.config.loader import load_config
        app_config = load_config(args.config, args.service, logger)

        from server.check.executor import CheckExecutor
        executor = CheckExecutor(app_config, logger)
        executor.run()


if __name__ == "__main__":
    main()
