#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
from enum import Enum

import sqlparse

from logging import Logger


class State(Enum):
    CODE = 0  # 代码
    SLASH = 1  # 斜杠
    NOTE_MULTILINE = 2  # 多行注释
    NOTE_MULTILINE_STAR = 3  # 多行注释遇到*
    NOTE_SINGLELINE = 4  # 单行注释
    BACKSLASH = 5  # 拆行注释
    CODE_CHAR = 6  # 字符
    CHAR_ESCAPE_SEQUENCE = 7  # 字符中的转义字符
    CODE_STRING = 8  # 字符串
    STRING_ESCAPE_SEQUENCE = 9  # 字符串中的转义字符
    STRIGULA = 10  # 短横线

class SqlUtil:
    def __init__(self, logger: Logger):
        self.logger = logger

    def extract_definitions(self, token_list):
        # assumes that token_list is a parenthesis
        definitions = []
        tmp = []
        par_level = 0
        for token in token_list.flatten():
            if token.is_whitespace:
                continue
            elif token.match(sqlparse.tokens.Punctuation, '('):
                par_level += 1
                continue
            if token.match(sqlparse.tokens.Punctuation, ')'):
                if par_level == 0:
                    break
                else:
                    par_level += 1
            elif token.match(sqlparse.tokens.Punctuation, ','):
                if tmp:
                    definitions.append(tmp)
                tmp = []
            else:
                tmp.append(token)
        if tmp:
            definitions.append(tmp)
        return definitions

    def clean_virtual_generated_column(self, sql_statement: str):
        # 正则表达式匹配类型为VIRTUAL GENERATED的字段
        pattern = r'(\s*`.+`[^,]+GENERATED\s+ALWAYS\s+AS\s+\(.*\) VIRTUAL,?)'

        # 移除匹配到的字段
        cleaned_table_sql = re.sub(pattern, '', sql_statement, flags=re.IGNORECASE)
        return cleaned_table_sql

    def get_target_file_schema(self, path: str):
        """
        根据sql文件路径，返回一个sql语句的列表
        :param path: 绝对路径
        :return:
        """
        # 解析文件
        # with open(path, 'r') as f:
        #     sql = f.read()
        # res_sql_list = []
        # res_sql_list.append(sql)
        # --------------分割符-------------------------
        try:
            path_file = path
            fp = open(path_file, 'r', encoding='utf-8')
            self.logger.info(f"parse sql file path is {path_file}")
        except IOError as ex:
            err = f"Cannot open file failed, err: {str(ex)}"
            raise Exception(err)

        sql_str = fp.read()
        sql_str = sqlparse.format(sql_str, strip_comments=True)
        sql_list = sqlparse.split(sql_str)
        sql_list = [sql for sql in sql_list if sql != ";"]
        return sql_list

    def sql_parse(self, sql_statement: str):
        """解析create语句中，对于字段的定义"""
        # 清理文件注释
        sql_statement = self.clean_comment_sql(sql_statement)
        # 清理字段注释中的--
        sql_statement = self.clean_column_comment_sql(sql_statement)
        # 清理虚拟生成列
        sql_statement = self.clean_virtual_generated_column(sql_statement)
        # 生成字段列表
        parsed = sqlparse.parse(sql_statement)[0]
        _, par = parsed.token_next_by(i=sqlparse.sql.Parenthesis)
        columns = self.extract_definitions(par)
        return columns

    def clean_column_comment_sql(self, sql_str: str):
        """
        移除字段中的注释
        """
        cleaned_table_sql = sql_str.replace("--", "")

        return cleaned_table_sql


    def clean_comment_sql(self, str: str):
        """
        移除comment中的--
        """
        s = ""
        # 初始状态定义为代码
        state = State.CODE
        for c in str:
            if state == State.CODE:
                # 遇到单斜杠
                if c == '/':
                    # 将状态改为单斜杠
                    state = State.SLASH
                # 遇上短横线
                elif c == "-":
                    state = State.STRIGULA
                else:
                    s += c
                    # 在代码中遇到字符
                    if c == '\'':
                        state = State.CODE_CHAR
                    # 在代码中遇到字符串
                    elif c == '\"':
                        state = State.CODE_STRING

            # 如果遇到单横线
            elif state == State.STRIGULA:
                # 后面接着跟一个单横线，则说明是单行注释
                if c == '-':
                    state = State.NOTE_SINGLELINE
                # 如果后面跟的是其他字符，则将状态转为代码
                else:
                    s += "-" + c
                    state = State.CODE

            # 遇到单斜杠
            elif state == State.SLASH:
                # 单斜杠后面再遇到*号，则说明是多行注释
                if c == '*':
                    state = State.NOTE_MULTILINE
                # 单斜杠后面再遇到单斜杠，说明是单行注释
                elif c == '/':
                    state = State.NOTE_SINGLELINE
                # 如果是其他，则将其添加到字符串中
                else:
                    s += "/"
                    s += c
                    state = State.CODE

            # 遇到多行注释
            elif state == State.NOTE_MULTILINE:
                # 多行注释后面遇到*号
                if c == '*':
                    state = State.NOTE_MULTILINE_STAR
                # 多行注释后面换行了
                else:
                    if c == '\n':
                        s += '\r\n'
                    # 则当前状态还是多行注释
                    state = State.NOTE_MULTILINE
            # 多行注释后面又遇到*号
            elif state == State.NOTE_MULTILINE_STAR:
                # 如果*号后面又是单斜杠在，则说明注释结束了
                if c == '/':
                    # 将状态改为代码
                    state = State.CODE
                # 如果*号后面又是*号，则说明还是多行注释遇到*
                elif c == '*':
                    state = State.NOTE_MULTILINE_STAR
                # 其他情况还是多行注释
                else:
                    state = State.NOTE_MULTILINE
            # 单行注释
            elif state == State.NOTE_SINGLELINE:
                if c == '\\':
                    state = State.BACKSLASH
                # 如果遇到换行符，则说明单行注释结束了
                elif c == '\n':
                    s += '\r\n'
                    state = State.CODE
                # 如果是其他情况，则说明单行注释还没有结束
                else:
                    state = State.NOTE_SINGLELINE
            # 拆行注释
            elif state == State.BACKSLASH:
                if c == '\\' or c == '\r' or c == '\n':
                    if c == '\n':
                        s += '\r\n'
                    state = State.BACKSLASH
                else:
                    state = State.NOTE_SINGLELINE
            # 代码中遇到单引号
            elif state == State.CODE_CHAR:
                s += c
                # 字符串中的转义字符
                if c == '\\':
                    state = State.CHAR_ESCAPE_SEQUENCE
                # 再次遇到单引号，则说明字符串内容结束了。
                elif c == '\'':
                    state = State.CODE
                # 如果是其他情况，则还是在字符串中
                else:
                    state = State.CODE_CHAR
            # 字符串中遇到转义字符
            elif state == State.CHAR_ESCAPE_SEQUENCE:
                s += c
                state = State.CODE_CHAR
            # 字符串
            elif state == State.CODE_STRING:
                s += c
                if c == '\\':
                    state = State.STRING_ESCAPE_SEQUENCE
                # 字符串内容结束
                elif c == '\"':
                    state = State.CODE
                else:
                    state = State.CODE_STRING
            elif state == State.STRING_ESCAPE_SEQUENCE:
                s += c
                state = State.CODE_STRING
        return s

    def format_sql(self,sql: str):
        """
        格式化sql语句
        """
        return sqlparse.format(sql, reindent_aligned=True)