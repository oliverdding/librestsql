from restsql.datasource.pg_client import PgClient
import json


class DataBase:
    db_name = "test"
    host = "localhost"
    port = "5432"
    user = "postgres"
    password = "12345"


class Query:
    def __init__(self, query_dict):
        """
        :param query_dict: 请求协议字典
        """
        self.From = query_dict["from"]
        self.time_dict = query_dict["time"]
        self.select_list = query_dict["select"]
        self.where_list = query_dict["where"]
        self.group_list = query_dict["group"]
        self.limit = query_dict["limit"]


json_sql = '''
{
    "from":"test.sale",
    "time":{
      "column":"create_time",
      "begin":"2021-7-20",
      "end":"2021-7-30",
      "interval":"86400"
    },
    "select":[
      {
        "column":"=price",
        "alias":"平均金额",
        "metric":"avg"
      },
      {
        "column":"price",
        "alias":"总额",
        "metric":"sum"
      },
      {
        "column":"price",
        "alias":"订单数",
        "metric":"count"
      }
    ],
    "where":[],
    "group":[],
     "limit":1000
  }
'''
database = DataBase()
query = Query(json.loads(json_sql))
pg = PgClient(database)
print(pg.query(query))
