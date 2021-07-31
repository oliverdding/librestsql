class PostgreDatabase:
    """
    作为postgre模板类
    """
    def __init__(self, db_name, user, password, host, port):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port


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
        self.From = query_dict["from"]
        self.time_dict = query_dict["time"]
        self.select_list = query_dict["select"]
        self.where_list = query_dict["Where"]
        self.group_list = query_dict["group"]
        self.limit = query_dict["limit"]


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
