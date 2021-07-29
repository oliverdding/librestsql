# encoding=utf-8
from restsql.datasource.es.es_db_setting import *
from restsql.datasource.es.es_entry import *
from restsql.config.db_setting import *


class restClient:
    """
    restsql主要服务类，服务器端调用，输入请求协议，输出DataFrame
    内部实现：
    根据请求协议识别查询的数据源，通过调用相应数据源的Client服务类，输出DataFrame
    """
    def __init__(self,query_dict,datasource):
        self.datasource=datasource
        self.query_dict = query_dict

    #供服务器端调用的接口
    def query(self):
        # db_name=self.query_dict["from"].split(".")[0]

        #获取DataBase对象
        db_setting=self.datasource['db_setting']
        db_name=db_setting['dbname']

        # 添加db对象
        self.add_database_bydbname(self,db_setting)
        # 获取db配置对象
        dataBase=db_configs.get_by_dbname(db_name)
        if dataBase.db_type==EnumDataBase.ES:
            client=EsClient(dataBase)
            indexname=self.datasource['table'].keys()[0]
            if indexname:
                return client.es_query(self.query_dict,indexname)
            return False

        # elif dataBase.db_type == EnumDataBase.PG:
        #     client = PgClient(dataBase)
        #     return client.pg_query(self.query_dict)
        # elif dataBase.db_type==EnumDataBase.DRUID:
        #     client=DruidClient(dataBase)
        #     return client.druid_query(self.query_dict)

    def add_database_bydbname(self,db):
        esdb_configs.put(DataBase(
            name=db['dbname'],
            db_type=EnumDataBase.ES,
            host=db['host'],
            port=db['port']
        ))

