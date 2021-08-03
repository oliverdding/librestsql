# encoding=utf-8

from restsql.config.database import EnumDataBase
from restsql.datasource.sql_entry import to_sql, get_columns
from sqlalchemy.exc import CompileError
from restsql.datasource.es_entry import EsQuery
import psycopg2
import pandas as pd

__all__ = ['Client', 'DruidClient', 'PgClient','EsClient']

from restsql.query import Query


class Client:
    def __init__(self, database):
        """
        :param database: dataBase对象
        """
        self.database = database

    def query(self, que):
        """
        :param que: 请求协议的封装类Query对象
        :return: DataFrame格式数据
        """
        raise NotImplementedError
        pass


class DruidClient(Client):
    """
    Druid数据源
    """

    def query(self, que):
        sql, param_dic = to_sql(que, EnumDataBase.DRUID, self.database.schema)
        try:
            conn = self.database.connect_db()
            curs = conn.cursor()
            curs.execute(sql, param_dic)
        except CompileError as e:
            raise e
        res = curs.fetchall()
        columns = get_columns(que)
        return pd.DataFrame(data=res, columns=columns)


class PgClient(Client):
    """
    提供给外界的接口类
    """

    def query(self, que: Query):
        sql, param_dic = to_sql(que, EnumDataBase.PG, self.database.schema)
        conn = self.database.connect_db()
        columns = get_columns(que)
        # 调用数据库，得到sql语句查询的结果
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, param_dic)
                rows = cursor.fetchall()
        except psycopg2.Error as e:
            raise e
        # 以dataFrame格式返回
        res = pd.DataFrame(data=rows, columns=columns)
        return res


# 这段还未整合，等接下来，es端处理进行整合
class EsClient(Client):
    """
    Es数据源服务类，供restSqlClient调用
    """

    # 这里为暴露的接口，供进行调用,统一返回dateframe
    def query(self, que):
        alias_dict = {}
        for s in que.select_list:
            alias_dict[s["column"]] = s["alias"]
        alias_dict[que.time_dict["column"]] = "time"
        results = []
        esQuery = EsQuery(que)
        index = que.target.split(".")[1]
        dsl = esQuery.parse()
        raw_result = self.database.connect_db().search(index=index, body=dsl)
        if 'aggs' in raw_result or 'aggregations' in raw_result:
            if raw_result.get('aggregations'):
                results = raw_result['aggregations']['groupby']['buckets']
            else:
                results = raw_result['agg']['groupby']['buckets']
            for it in results:
                it["time"] = it["key"][que.time_dict["column"]]
                del it['key']
                del it['doc_count']
                for field, value in it.items():
                    if isinstance(value, dict) and 'value' in value:
                        it[field] = value['value']
        elif 'hits' in raw_result and 'hits' in raw_result['hits']:
            for it in raw_result['hits']['hits']:
                record = it['_source']
                result = {}
                for field in record.keys():
                    if alias_dict[field] == "":
                        result[field] = record[field]
                    else:
                        result[alias_dict[field]] = record[field]
                results.append(result)
        # 关闭连接操作
        self.database.connect_db().close()
        return pd.DataFrame(results)

