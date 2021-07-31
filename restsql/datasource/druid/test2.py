import json
from pydruid.db import connect

testsql = """{
  "from":"",
  "select":[
    {
      "column":"__time",
      "alias":"mytime",
      "metric":"COUNT"
    }
  ],
  "where":[
    {
      "column":"added",
      "op":">",
      "value": 0
    }
  ],
  "group":["__time"],
   "limit":1000
} """

import pandas as pd

testjson = json.loads(testsql)
print(testjson)


class Query:
    def __init__(self, restquery):
        self.restquery = restquery
        self._result = None
        self.model = QueryModel()

    def sql_do_query(self):
        # 调用self.query进行操作 ,然后把字符串结果进行存储到  _result里面进去
        # 接受的query为json字符串
        print(self.restquery)
        print(1)
        from_sub = self.restquery['from']
        # TODO错误处理
        tablename = "\"wikipedia\""
        # if tablename in self.database.black_tables:
        #     raise RuntimeError("the table in the blacktables")

        # TODO错误处理，比如这个是不是数组，或者有无字段，及其错误
        select_dict = self.restquery['select']
        where_dict = self.restquery["where"]
        group_dict = self.restquery['group']

        if not select_dict or len(select_dict) < 1:
            raise RuntimeError("the select")

        time = self.restquery.get('time', None)
        select_sql = self.parse_select(select_dict, time)
        sql="SELECT {} FROM {}".format(select_sql, tablename)

        if where_dict:
            sql += self.parse_where(where_dict, time)
        group_sql = self.parse_group()
        if group_dict:
            sql += self.parse_group()
        print(sql)
        print("总的语句")

        # 开始执行
        cur = connect(host='127.0.0.1', port='8888', path='/druid/v2/sql', scheme='http').cursor()
        cur.execute(sql)
        print(cur.fetchall())

    def parse_select(self, self_dict, time):
        print(self_dict)
        print(2)
        templist = []
        if time:
            templist.append(self.model.get_time_model(time['column'], time['interval']) + ' and ')

        for item in self_dict:
            str = "{} {},"
            if len(item.get('column', "")) < 1:
                raise RuntimeError("empty column")
            if len(item.get('metric', "")) < 1:
                metric = item['column']
            else:
                metric = self.model.get_metric_model(item['metric'],item['column'])
            if len(item.get('alias', "")) < 1:
                str.format(metric, "")
                continue
            templist.append(str.format(metric, "as " + item['alias']))

            selecttst=''.join(templist)[:-1]
            print(selecttst)
            print("selectresult")
            return selecttst

    def parse_where(self, where_dict, time):
        templist = [" where "]  # 首先先处理时间的
        if time:
            if time['begin'] > time['end']:
                raise RuntimeError("the time error")
            templist.append(time['column'] + " between \'" + time['begin'] + "\' and \'" + time['end'] + "\'")

        # TODO 是否要进行条件查询，有四则运算的
        for i in range(0, len(where_dict)):
            if len(where_dict[i].get('column', "")) > 0 and len(where_dict[i].get('op', "")) > 0 and len(
                    where_dict[i].get('column', "")) > 0:
                templist.append("\"{}\" {} {}".format(where_dict[i]['column'], where_dict[i]['op'], where_dict[i]['value']))
            elif len(where_dict[i].get('op', "")) > 0:
                templist.append(where_dict[i]['op'])
            else:
                raise RuntimeError("param error!")

        result_where=''.join(templist)
        print(result_where)
        print("where")
        return result_where

    def parse_group(self):
        group_sub = " group by "
        for item in self.restquery['group']:
            group_sub +=item+","
            #要分情况来判断要不要加冒号
        return group_sub[:-1]

    @property  # 调用方法名，不用加()
    def result(self):
        return self._result


class QueryModel:
    def __init__(self):
        self.metric_dict = {
            'SUM': 'sum({})',
            'AVG': 'avg({})',
            'TIME': 'xxxx',
            'COUNT': 'count({})'
        }
        self.group_dict = {
            'condition': '{}{}{}'
        }

    def get_time_model(self, time, inteval):
        a = 1

    def get_metric_model(self, metric,column):
        if not self.metric_dict.get(metric, None):
            raise RuntimeError("no find the metric method")
        return self.metric_dict.get(metric).format(column)


Qu = Query(testjson)
result = Qu.sql_do_query()
