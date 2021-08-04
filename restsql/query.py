# encoding=utf-8

__all__ = ['Query']
"""
请求协议的封装类
"""


class Query:
    def __init__(self, query_dict):
        """
        :param query_dict: 请求协议字典
        """
        self.target = query_dict.get("from", "")
        self.time_dict = query_dict.get("time", {})
        self.select_list = query_dict.get("select", [])
        self.where_list = query_dict.get("where", [])
        self.group_list = query_dict.get("group", [])
        self.limit = query_dict.get("limit", 1000)
