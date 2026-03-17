#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def set_migration_cmd(subparsers):
    parser_migrations = subparsers.add_parser("migrations", help="data schema migration")
    complete_mode_help = """
    Normal is an idempotent pattern.
    latest is the upgrade script for re executing the latest version of each service.
    rerun is a script for re upgrading a certain version.
    It is not recommended to use x and xx as these two modes can cause irreversible errors in the program.
    """
    online_upgrade_help = """
    true: Online mode completes uninterrupted updates to relational databases using pt-schema-change combined with DDL.
    false: Offline mode involves directly executing DDL, which may cause DML blocking in Galera clusters.
    """
    parser_migrations.add_argument('-m', '--mode', choices=['normal', 'latest', 'rerun'], help=complete_mode_help,  required=False)
    parser_migrations.add_argument('-ou', '--online_upgrade', choices=['true', 'false'], help=online_upgrade_help, required=False)
    parser_migrations.add_argument('-s', '--stage', choices=['pre', 'post'], help='enter migrations stage',  required=False)