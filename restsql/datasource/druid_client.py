import pandas as pd
from restsql.config.model import Client


class Query:
    def __init__(self, restquery, database):
        self.restquery = restquery
        self.database = database
        self._result = None
        self.model = QueryModel()

    def do_query(self):
        # 调用self.query进行操作 ,然后把字符串结果进行存储到  _result里面进去
        # 接受的query为json字符串
        from_sub = self.restquery['from'].split('.', 1)
        if len(from_sub) <2:
            raise RuntimeError("false 'from' column ")
        # TODO错误处理
        # tablename = "\"wikipedia\""
        tablename = from_sub[1]  # 这个只需要一个表名就行
        if tablename in self.database.black_tables:
            raise RuntimeError("the table in the blacktables")

        # TODO错误处理，比如这个是不是数组，或者有无字段，及其错误
        select_dict = self.restquery['select']
        where_dict = self.restquery["where"]
        group_dict = self.restquery['group']

        if not select_dict or len(select_dict) < 1:
            raise RuntimeError("the select")

        time = self.restquery.get('time', None)
        select_sql = self.parse_select(select_dict, time)
        sql = "SELECT {} FROM \"{}\"".format(select_sql, tablename)

        if where_dict:
            sql += self.parse_where(where_dict, time)
        group_sql = self.parse_group()
        if group_dict:
            sql += self.parse_group()

        # 开始执行
        cur = self.database.db.cursor()
        print(sql)
        cur.execute(sql)
        print(cur.fetchall())

    def parse_select(self, self_dict, time):

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
                metric = self.model.get_metric_model(item['metric'], item['column'])
            if len(item.get('alias', "")) < 1:
                str.format(metric, "")
                continue
            templist.append(str.format(metric, "as " + item['alias']))

            selecttst = ''.join(templist)[:-1]

            return selecttst

    def parse_where(self, where_dict, time):
        templist = [" where "]  # 首先先处理时间的
        if time:
            if time['begin'] > time['end']:
                raise RuntimeError("the time error")
            templist.append(time['column'] + " between \'" + time['begin'] + "\' and \'" + time['end'] + "\'")

        # TODO是否要进行条件查询，有四则运算的
        for i in range(0, len(where_dict)):
            if len(where_dict[i].get('column', "")) > 0 and len(where_dict[i].get('op', "")) > 0 and len(
                    where_dict[i].get('column', "")) > 0:
                templist.append(
                    "\"{}\" {} {}".format(where_dict[i]['column'], where_dict[i]['op'], where_dict[i]['value']))
            elif len(where_dict[i].get('op', "")) > 0:
                templist.append(where_dict[i]['op'])
            else:
                raise RuntimeError("param error!")

        result_where = ''.join(templist)
        return result_where

    def parse_group(self):
        group_sub = " group by "
        for item in self.restquery['group']:
            group_sub += item + ","
            # 要分情况来判断要不要加冒号
        return group_sub[:-1]

    @property  # 调用方法名，不用加()
    def result(self):
        return self._result


class DruidClient(Client):
    def __init__(self, datasource):
        self.datasource = datasource

    def druid_query(self, querysql):  # 数据源是固定的，但是table可能有变化
        query = Query(querysql, self.datasource)
        query.do_query()


class EnumMetric:
    SUM = 'sum({})'
    AVG = 'avg({})'


class QueryModel:
    def __init__(self):
        self.metric_dict = {
            'SUM': 'sum({})',
            'AVG': 'avg({})',
            'TIME': 'TIME_FLOOR({},{})',  # TODO时间处理在接下来进一步处理
            'COUNT': 'count({})'
        }
        self.group_dict = {
            'condition': '{}{}{}'
        }
        self.time_dict = {  # TImezone格式
            '1M': 'PT1M',
            '1H': 'PT1H',
        }

    def get_time_model(self, metric, time, inteval):
        return self.metric_dict.get(metric).format(time, self.time_dict.get(inteval, 'PT1S'))

    def get_metric_model(self, metric, column):
        if not self.metric_dict.get(metric, None):
            raise RuntimeError("no find the metric method")
        return self.metric_dict.get(metric).format(column)
