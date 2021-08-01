# encoding=utf-8

from restsql.config.settings import DataBase, EnumDataBase
from restsql.datasource.druid_client import DruidClient
from restsql.query import Query
import json

data_json = '''
{
    "from":"test.wikipedia",
    "time":{
      "column":"__time",
      "begin":"2015-7-20",
      "end":"2017-7-30",
      "interval":"1000"
    },
    "select":[
      {
        "column":"commentLength",
        "alias":"评论平均字数",
        "metric":"avg"
      },
      {
        "column":"commentLength",
        "alias":"评论总字数",
        "metric":"SUM"
      }
    ],
    "where":[],
    "group":[],
     "limit":100
  }
'''

database = DataBase(name='test', db_type=EnumDataBase.DRUID, port='8888', host='localhost')
client = DruidClient(database)
que = Query(json.loads(data_json))
print(client.query(que))
