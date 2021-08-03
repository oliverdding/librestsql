# encoding=utf-8

from restsql.config.settings import DataBase, EnumDataBase
from restsql.datasource.pg_client import PgClient
from restsql.query import Query
import json

data_json = '''
{
    "from":"test.sale",
    "time":{
      "column":"create_time",
      "begin":"2021-7-20",
      "end":"2021-7-30",
      "interval":"1d"
    },
    "select":[
      {
        "column":"price",
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
database = DataBase(name='test', db_type=EnumDataBase.PG, port='5432', db_name='test',
                    host='localhost', user='postgres', password='12345')
query = Query(json.loads(data_json))
client = PgClient(database)
print(client.query(query))
