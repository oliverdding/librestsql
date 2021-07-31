import re

from restsql.datasource.es import restClient
from restsql.datasource.postgre import sql_entry
from restsql.datasource.druid import druid_client
from restsql.config.dbsetting import db_settings


class Client:

    def __init__(self, query, pid):
        self.pid = pid
        self.querysql = query
        self._result = None

    def query(self):
        p = re.compile(r'\W+')  # test.table ，分离出单词
        sub = p.split(self.querysql['from'])
        datasource = db_settings.get_by_name(sub[0])
        dbtype = datasource.db_type
        if dbtype == 'Elasticsearch':
            client = restClient.restClient(self.querysql, datasource)
            self._result = client.query()
        elif dbtype == 'PostgreSQL':
            client = sql_entry.SQLClient(datasource)
            self._result = client.sql_query(self.querysql, self.pid)
        elif dbtype == 'Druid':
            client = druid_client.DruidClient(datasource)
            self._result = client.druid_query(self.querysql, self.pid)
        else:
            return False
        return True

    @property
    def result(self):
        return self._queryresult
