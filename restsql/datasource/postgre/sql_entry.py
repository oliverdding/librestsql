import pandas as pd
from . import Postgre


class Query:
    def __init__(self, query, database, table, table_struct, pid):
        self.query = query
        self.database = database
        self.pid = pid
        self._result = None
        self.tablename = table  # 指定表名
        # self.table=self._get_table() 如果有orm需要指定对象,进行使用

    # def _get_table(self): 有orm需要生成model，使用

    def sql_do_query(self):
        # 调用self.query进行操作 ,然后把字符串结果进行存储到  _result里面进去
        # 接受的query为json字符串
        print(self.database)
        self._result = Postgre.query(self.database, self.query, self.pid)

    @property  # 调用方法名，不用加()
    def result(self):
        return self._result


class SQLClient:
    def __init__(self, datasource):
        print("init sqlclient")
        self.datasource = datasource

    def set_datasource(self, datasource):
        self.datasource = datasource

    def sql_query(self, querysql, pid):  # 数据源是固定的，但是table可能有变化
        if len(self.datasource['table']) < 1:
            return False  # 举个例子，当用户没有传入表名的时候

        tablename = self.datasource['table'][0]['tablename']
        tablestruct = self.datasource['table'][0]['struct']
        query = Query(querysql, self.datasource, tablename, tablestruct, pid)
        query.sql_do_query()

        # result = query.result  # 获取对象
        # xxx进行额外处理
        result = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
        return pd.DataFrame(result, columns=['Name', 'Age'], dtype=float)
        # 统一返回dataframe