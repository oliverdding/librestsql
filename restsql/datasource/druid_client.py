# encoding=utf-8

from restsql.config.database import EnumDataBase
from restsql.datasource.util import to_sql, get_columns
from sqlalchemy.exc import CompileError
from restsql.config.model import Client
import pandas as pd


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
