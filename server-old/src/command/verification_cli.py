#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def set_verification_cmd(subparsers):

    subparsers.add_parser("verification", help="data schema verification")

    # parser_verification.add_argument('-db', '--db_type', help='input database type, default:mysql', required=False)
    # parser_verification.add_argument('-p', '--password', help='database account password', required=False)
    # parser_verification.add_argument('-P', '--port', help='database port', required=False)
    # parser_verification.add_argument('-h', '--host', help='database host', required=False)
    # parser_verification.add_argument('-t', '--dbtype', help='database type', required=False)
    # parser_verification.add_argument('-sdp', '--script_directory_path', help='Path to pass in upgrade script, [default] /app/repos/', required=False)