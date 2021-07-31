import re

from restsql.config.database import db_settings, EnumDataBase
from restsql.datasource.pg_client import PgClient
from restsql.datasource.es_client import EsClient
from restsql.datasource.druid_client import DruidClient


class Client:

    def __init__(self):
        self._result = None

    def query(self, querysql):
        p = re.compile(r'\W+')  # test.table ，分离出单词
        sub = p.split(querysql['from'])
        datasource = db_settings.get_by_name(sub[0])
        dbtype = datasource.db_type
        if dbtype == EnumDataBase.ES:
            client = EsClient(datasource)
            self._result = client.query(querysql)
            self._result = client.query()
        elif dbtype == EnumDataBase.PG:
            client = PgClient(datasource)
            self._result = client.query(querysql)
        elif dbtype == EnumDataBase.DRUID:
            client = DruidClient(datasource)
            self._result = client.druid_query(querysql)
        else:
            return False
        return True

    @property
    def result(self):
        return self._queryresult
