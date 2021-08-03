# encoding=utf-8
import logging
from restsql.datasource.es_entry import *
from restsql.query import Query
from restsql.datasource.util import *
from restsql.config.database import db_settings, EnumDataBase
from restsql.datasource.client import PgClient, DruidClient,EsClient

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
        _check_field(self.query_instance)
        db_name = self.query_instance.From.split(".")[0]
        # 获取DataBase对象
        database = db_settings.get_by_dbname(db_name)
        if database.db_type == EnumDataBase.ES:
            client = EsClient(database)
        elif database.db_type == EnumDataBase.PG:
            client = PgClient(database)
        else:
            client = DruidClient(database)
        return client.query(self.query_instance)
