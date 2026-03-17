#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import re
from typing import List


def compare_version(v1, v2):
    """
    适配当前三种版本号的方法
    比较两个版本号的大小，返回-1、0、1分别代表v1小于、等于、大于v2
    """
    # 首先尝试匹配第一类版本号格式，例如m5.5、m5.6等等
    match1 = re.match(r"m(\d+)\.(\d+)", v1)
    match2 = re.match(r"m(\d+)\.(\d+)", v2)
    if match1 and match2:
        major1, minor1 = int(match1.group(1)), float(match1.group(2))
        major2, minor2 = int(match2.group(1)), float(match2.group(2))
        if major1 < major2:
            return -1
        elif major1 > major2:
            return 1
        else:
            if minor1 < minor2:
                return -1
            elif minor1 > minor2:
                return 1
            else:
                return 0
    # 如果第一类版本号格式不匹配，则尝试匹配第二类版本号格式，例如m5-5、m5-6等等
    match1 = re.match(r"m(\d+)-(\d+)", v1)
    match2 = re.match(r"m(\d+)-(\d+)", v2)
    if match1 and match2:
        major1, minor1 = int(match1.group(1)), int(match1.group(2))
        major2, minor2 = int(match2.group(1)), int(match2.group(2))
        if major1 < major2:
            return -1
        elif major1 > major2:
            return 1
        else:
            if minor1 < minor2:
                return -1
            elif minor1 > minor2:
                return 1
            else:
                return 0
    try:
        arr1 = [int(n) for n in v1.split('.')]
        arr2 = [int(n) for n in v2.split('.')]

        # 将两个数组补齐至相同长度
        while len(arr1) < len(arr2):
            arr1.append(0)
        while len(arr1) > len(arr2):
            arr2.append(0)

        # 依次比较对应的版本号数字大小
        for i in range(len(arr1)):
            if arr1[i] > arr2[i]:
                return 1
            elif arr1[i] < arr2[i]:
                return -1

        # 如果版本号数字全部相同，则返回 0
        return 0
    except Exception as ex:
        raise Exception(f"Unable to parse version number,details is {ex}")


def get_max_version(versions):
    """
    给定版本号列表，返回最新的版本号
    """
    if not versions:
        return None
    # 初始化最大版本号为列表中的第一个版本号
    max_v = versions[0]
    for version in versions[1:]:
        # 如果当前版本号比最大版本号大，则更新最大版本号
        if compare_version(version, max_v) > 0:
            max_v = version
    return max_v


def insertion_sort(arr):
    """自定义插入排序"""
    for i in range(1, len(arr)):
        key = arr[i]
        j = i - 1

        while j >= 0 and compare_version(arr[j], key) > 0:
            arr[j+1] = arr[j]
            j -= 1
        arr[j+1] = key
    return arr


def extract_number(file_path: str):
    """匹配.sql和.py文件文件"""
    s = file_path.split("/")[-1]
    match1 = re.search(r'^(\d+)-.*\.sql$', s)
    match2 = re.search(r'^(\d+)-.*\.py$', s)
    match3 = re.search(r'^(\d+)-.*\.json$', s)
    if match1:
        return int(match1.group(1))
    elif match2:
        return int(match2.group(1))
    elif match3:
        return int(match3.group(1)) 
    else:
        raise Exception(f"The upgrade file type must be sql|py|json, filename:{s}")

def get_min_version(versions: List[str]):
    """
    给定版本号列表，返回本次打包最小的版本号
    """
    if not versions:
        return None
    # 初始化最大版本号为列表中的第一个版本号
    min_v = versions[0]
    for version in versions[1:]:
        # 如果当前版本号比最大版本号大，则更新最大版本号
        if compare_version(version, min_v) < 0:
            min_v = version
    return min_v

def remove_elements_between_comments(lst: List[str]) -> List[str]:
    """移除多行注释"""
    stack = []
    result = []

    for elem in lst:
        if "/*" in elem and "/*!" not in elem:
            stack.append("/*")

        if not stack:
            result.append(elem)

        if "*/" in elem:
            if stack:
                stack.pop()

    return result

def get_target_file_schema(path: str) -> List:
    """
    根据sql文件路径，返回包含所有sql语句的列表
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
    except IOError as ex:
        err = f"Cannot open file failed, err: {str(ex)}"
        raise Exception(err)

    lines = fp.readlines()
    schema_file = remove_elements_between_comments(lines)

    # try:
    results, results_list = [], []
    # 去除\n\r
    if len(schema_file) > 0 and schema_file[-1].endswith(";"):
        schema_file[-1] = schema_file[-1] + "\n"
    for sql in schema_file:
        if sql.startswith("\n") or sql == "\r":
            continue
        if sql.startswith("--"):
            continue
        # 获取不是“--”开头且不是“--”结束的数据
        if not sql.startswith("--") and not sql.endswith("--"):

            results.append(sql)
    temp_res_sql_list, res_sql_list = [], []
    while len(results) > 0:
        for i in range(len(results)):
            if results[i].endswith(";\n"):
                tem_str = "".join(results[:i + 1])
                temp_res_sql_list.append(tem_str)
                del results[:i + 1]
                break
    for i in temp_res_sql_list:
        if i.endswith(";\n"):
            i = i.strip()
            i = i[::-1].replace(";", "", 1)[::-1]
            res_sql_list.append(i)
    return res_sql_list