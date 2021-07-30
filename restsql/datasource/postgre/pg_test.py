import Postgre

database = {'dbname': 'test', 'host': 'localhost', 'port': '5432', 'username':'postgres', 'password': '12345'}
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
Postgre.query(database, json_sql, '1')
