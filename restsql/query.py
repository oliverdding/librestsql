# encoding=utf-8

"""
请求协议的封装类
"""


class Query:
    def __init__(self, query_dict):
        """
        :param query_dict: 请求协议字典
        """
        self.From = query_dict["from"]
        self.time_dict = query_dict["time"]
        self.select_list = query_dict["select"]
        self.where_list = query_dict["where"]
        self.group_list = query_dict["group"]
        self.limit = query_dict["limit"]
