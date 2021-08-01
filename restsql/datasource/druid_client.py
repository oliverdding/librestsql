# encoding=utf-8

from restsql.config.settings import EnumDataBase
from restsql.datasource.util import to_sql, get_columns
from sqlalchemy.exc import CompileError
import pandas as pd

__all__ = ['Client']


class Client:
    def __init__(self, database):
        self.database = database
        # raise NotImplementedError

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
        sql = to_sql(que, EnumDataBase.DRUID)
        try:
            conn = self.database.connect_db()
            curs = conn.cursor()
            curs.execute(sql)
        except CompileError as e:
            raise e
        res = curs.fetchall()
        columns = get_columns(que)
        return pd.DataFrame(data=res, columns=columns)
