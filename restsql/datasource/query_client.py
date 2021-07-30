# {
#     'db_setting':{
#         "dbname":xxx
#         "host":xxx
#         "password":xxx
#
#     }
#     'table':[
#             {
#                 'tablename': xxx
#                 'struct': {
#                     'column1': xxx(int)
#                     'column2': xxx(String)
#                 }
#                 'blackcolumn': []
#              }
#    ]
#     'blacktables':[]
# }
import re

from restsql.config import utils
from restsql.datasource.es import restClient
from restsql.datasource.sql import sql_entry


class Client:

    def __init__(self, fromsub, pid):
        self.source = {'db_setting': {}, 'table': [], 'blacktables': []}
        self.pid = pid
        self.from_sub = fromsub
        self._queryresult = None

    def query(self, query):
        p = re.compile(r'\W+')  # test.table ，分离出单词
        sub = p.split(self.from_sub)
        db = utils.get_datasource_bysource(sub[0])
        if not db:
            return False
        # 查找黑名单名字
        blacktables = db['blacktables']
        table = utils.get_table_bydbname(sub[0])  # 获取这个数据库的表名映射
        real_tablename = utils.get_realname_bytable(table, sub[1])  # 获取一个表名的真实表名

        self.source['blacktables'] = blacktables
        self.source['db_setting'] = db
        self.source['table'].append(self.get_temptable(sub[0], real_tablename))
        # if blacktables and blacktables.count(real_tablename):
        #     return False
        typeparam = db['type']
        if typeparam == 'es':
            client = restClient.restClient(query, self.source)
            self._queryresult = client.query()
            print(self.result)  # 尝试打印临时结果
        elif typeparam == 'sql':
            client = sql_entry.SQLClient(self.source)
            self._queryresult = client.sql_query(query, self.pid)
            print(self.result)
        elif typeparam == 'druid':
            a = 1
        else:
            return False
        return True

    def get_temptable(self, dbname, tablename):
        temp_dict = {'tablename': tablename, 'struct': utils.get_struct_bytable(dbname, tablename),
                     'blackcolumn': utils.get_blackcolumn_bytable(dbname, tablename)}
        return temp_dict

    @property
    def result(self):
        return self._queryresult
