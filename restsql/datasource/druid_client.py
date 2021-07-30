# encoding=utf-8


__all__ = ['Client']


class Client:
    def __init__(self, database):
        self.dataBase = database
        # raise NotImplementedError

    def query(self, query):
        """
        :param query: 请求协议的封装类Query对象
        :return: DataFrame格式数据
        """
        raise NotImplementedError
        pass


class DruidClient(Client):
    """
    TODO druid数据源实现
    """
    pass
