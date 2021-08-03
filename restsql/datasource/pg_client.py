# encoding=utf-8

from restsql.config.model import Client
from restsql.config.database import EnumDataBase
from restsql.datasource.utils import get_columns, to_sql
from restsql.config.model import Query
import pandas as pd
import psycopg2


class PgClient(Client):
    """
    提供给外界的接口类
    """

    def query(self, que: Query):
        sql = to_sql(que, EnumDataBase.PG)
        conn = self.database.connect_db()
        columns = get_columns(que)
        # 调用数据库，得到sql语句查询的结果
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
        except psycopg2.Error as e:
            raise e
        # 以dataFrame格式返回
        res = pd.DataFrame(data=rows, columns=columns)
        return res
