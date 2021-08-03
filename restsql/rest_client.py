# encoding=utf-8
from restsql.datasource.sql_entry import *
from restsql.config.database import *
from restsql.datasource.client import *
from restsql.check import *

__all__ = ['RestClient']


class RestClient:
    """
    restsql主要服务类，服务器端调用，输入请求协议，输出DataFrame
    内部实现：
    根据请求协议识别查询的数据源，通过调用相应数据源的Client服务类，输出DataFrame
    """

    def __init__(self, query_dict):
        self.query_instance = Query(query_dict)

    # 供服务器端调用的接口
    def query(self):
        # 进行格式检查，过滤掉非法字符，避免sql注入
        table_name = self.query_instance.target.split(".")[1]
        print(table_name)
        _check(self.query_instance, table_name)
        if self.query_instance.target is None:
            raise RuntimeError("The query target is empty")
        db_name = self.query_instance.target.split(".")[0]
        # 获取DataBase对象
        database = db_settings.get_by_name(db_name)
        if database.db_type == EnumDataBase.ES:
            client = EsClient(database)
        elif database.db_type == EnumDataBase.PG:
            client = PgClient(database)
        else:
            client = DruidClient(database)
        return client.query(self.query_instance)
