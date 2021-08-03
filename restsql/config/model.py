class DruidDatabase:
    def __init__(self, host, port):
        """
        :host Druid数据源地址
        :port Druid 数据源端口
        """
        self.host = host
        self.host = port


class Query:
    def __init__(self, query_dict):
        """
        :param query_dict: 请求协议字典
        """
        self.From = query_dict.get("from", None)
        self.time_dict = query_dict.get("time", {})
        self.select_list = query_dict.get("select", [])
        self.where_list = query_dict.get("where", [])
        self.group_list = query_dict.get("group", [])
        self.limit = query_dict.get('limit', None)


class Client:
    def __init__(self, database):
        """
        :param database:指定数据库配置对象
        """
        self.dataBase = database
        raise NotImplementedError

    def query(self, query):
        """
        :param query: 请求协议的封装类Query对象
        :return: DataFrame格式数据
        """
        raise NotImplementedError
        pass
