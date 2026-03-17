#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse

from dataModelManagement.src.command.migrations_cli import set_migration_cmd
from dataModelManagement.src.command.verification_cli import set_verification_cmd
from dataModelManagement.src.handler.handler import Handler


def cli():
    parser = argparse.ArgumentParser(description='Database schema change tool', prog='data-model-management [options]  [command] [sub-options]')
    parser.add_argument('-u', '--username', help='database user, It is best to have root privileges', required=False)
    parser.add_argument('-p', '--password', help='database account password', required=False)
    parser.add_argument('-P', '--port', help='database port', required=False)
    parser.add_argument('-H', '--host', help='database host', required=False)
    parser.add_argument('-t', '--type', help='database type', required=False)
    parser.add_argument('-ak', '--admin_key', help='database admin_key', required=False)
    parser.add_argument('-sdp', '--script_directory_path', help='Path to pass in upgrade script, [default] /app/repos/', required=False)
    parser.add_argument('-em', '--env_mode', help='is_production, prod, dev or tiduyun', required=False)
    parser.add_argument('-l', '--log_level', help='set log level', required=False)
    parser.add_argument('-sid', '--system_id', help='set system_id for db name', required=False)
    parser.add_argument('-st', '--source_type', help='set db source type', required=False)

    subparsers = parser.add_subparsers(dest='subcommand', title='subcommands', description='subcommands')
    set_migration_cmd(subparsers)
    set_verification_cmd(subparsers)
    subparsers.add_parser("version", help="gitcommit version")
    args = parser.parse_args()

    if args.subcommand == 'migrations':
        print("Running the set_migration_cmd...")
    elif args.subcommand == 'verification':
        print("Rerunning the set_verification_cmd...")
    return parser


def codebase_version():
    return "unknow"

def main():
    cli_info = cli()
    parse_args = cli_info.parse_args()
    if not parse_args.subcommand:
        cli_info.print_help()
        return
    elif parse_args.subcommand == 'version':
        commit_id = codebase_version()
        print(f"commit_id: {commit_id}")
    elif parse_args.subcommand == 'verification':
        Handler(parse_args).verification_run()
    else:
        Handler(parse_args).migration_run()


if __name__ == '__main__':
    main()
