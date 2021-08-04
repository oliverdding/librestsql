import pandas as pd
import json


_all_ = ['EsQuery']


class EsQuery:
    """
    将请求协议转译为Es—DSL查询语句
    """

    def __init__(self, query):
        """
        :param query: 请求协议封装对象
        格式：{
              "from":"",
              "time":{
                "column":"",
                "begin":"",
                "end":"",
                "interval":""
              },
              "select":[
                {
                  "column":"",
                  "alias":"",
                  "metric":""
                }
                .....
        """
        self.limit = 1000
        if query.limit is not None:
            self.limit = query.limit
        self.index = query.target.split(".")[1]
        self.select_list = query.select_list
        self.time = query.time_dict
        self.group_list = query.group_list
        self.where_list = query.where_list
        self.dsl = {
            'size': 1000,  # default
            'query': {
                'bool': {
                    'must': []
                }
            },
            '_source': {
                'includes': []
            },
            'aggs': {
                'groupby': {
                    "composite": {
                        "sources": [

                        ]
                    },
                    'aggs': {}
                }
            },
        }
        self.dsl_where = self.dsl['query']['bool']['must']
        self.dsl_composite = self.dsl['aggs']['groupby']['composite']['sources']
        self.dsl_aggs = self.dsl['aggs']['groupby']['aggs']

    def _parse_fields(self):
        if self.time["interval"] is None or self.time["interval"] == "":
            if len(self.group_list) == 0:
                self.dsl['_source']['includes'].append(self.time["column"])
        for s in self.select_list:
            self.dsl['_source']['includes'].append(s["column"])

    def _parse_where(self):
        """
        过滤操作暂时仅支持了and
        后续商讨后进行修改
        :return:
        """
        for filter_dic in self.where_list:
            if filter_dic["op"] == "=":
                self.dsl_where.append({
                    'term': {
                        filter_dic["column"]: filter_dic["value"]
                    }
                })
            elif filter_dic["op"] == "!=":
                self.dsl_where.append({
                    "bool": {
                        "must_not": [
                            {
                                "term": {
                                    filter_dic["column"]: filter_dic["value"]
                                }
                            }
                        ]
                    }})
            elif filter_dic["op"] == "<":
                self.dsl_where.append({
                    'range': {
                        filter_dic["column"]: {'lt': filter_dic["value"]}
                    }
                })
            elif filter_dic["op"] == ">":
                self.dsl_where.append({
                    'range': {
                        filter_dic["column"]: {'gt': filter_dic["value"]}
                    }
                })
            elif filter_dic["op"] == "<=":
                self.dsl_where.append({
                    'range': {
                        filter_dic["column"]: {'lte': filter_dic["value"]}
                    }
                })
            elif filter_dic["op"] == ">=":
                self.dsl_where.append({
                    'range': {
                        filter_dic["column"]: {'gte': filter_dic["value"]}
                    }
                })
            elif filter_dic["op"] == 'contains':
                self.dsl_where.append({
                    'wildcard': {filter_dic["column"]: ''.join(['*', filter_dic["value"], '*'])}
                })
            elif filter_dic["op"] == 'startswith':
                self.dsl_where.append({
                    'prefix': {filter_dic["column"]: filter_dic["value"]}
                })
            elif filter_dic["op"] == 'endswith':
                self.dsl_where.append({
                    'wildcard': {filter_dic["column"]: ''.join(['*', filter_dic["value"]])}
                })
            elif filter_dic["op"] == 'in':
                self.dsl_where.append({
                    'terms': {filter_dic["column"]: filter_dic["value"]}
                })
            elif filter_dic["op"] == 'not in':
                self.dsl_where.append({
                    "bool": {
                        "must_not": [
                            {
                                "terms": {
                                    filter_dic["column"]: filter_dic["value"]
                                }
                            }
                        ]
                    }})
            else:
                raise SyntaxError('cat not support op: {0}, field: {1}'.format(filter_dic["op"], filter_dic["column"]))
            # 请求协议输入必须为yyyy-MM-dd之类的格式且必须补足位数
        if self.time["begin"] is not None and self.time["begin"] != "":
            self.dsl_where.append({
                'range': {
                    self.time["column"]: {'gte': self.time["begin"]}
                }
            })
        if self.time["end"] is not None and self.time["end"] != "":
            self.dsl_where.append({
                'range': {
                    self.time["column"]: {'lte': self.time["end"]}
                }
            })
        if len(self.dsl_where) == 0:
            del self.dsl["query"]

    def _parse_metric(self):
        func_map = {'count': 'value_count','sum': 'sum', 'avg': 'avg', 'max': 'max', 'min': 'min',
                    'count distinct': 'cardinality'}
        for s in self.select_list:
            if s["metric"] in func_map.keys() and s["metric"]!="count":
                self.dsl_aggs[s["alias"]] = {func_map[s["metric"]]: {'field': s["column"]}}
            elif s["metric"]=="count":
                self.dsl_aggs[s["alias"]] = {func_map[s["metric"]]: {'field': s["column"]+".keyword"}}
            else:
                if s["metric"] == "" or s["metric"] is None:
                    continue
                raise SyntaxError('cat not support aggregation operation: {}'.format(s["metric"]))
        pass

    def _parse_composite(self):
        for g in self.group_list:
            sources_dict = {g: {"terms": {"field": g + ".keyword"}}}
            self.dsl_composite.append(sources_dict)
        if self.time["interval"] is not None and self.time["interval"] != "":
            sources_dict = {self.time["column"]: {"date_histogram": {"field": self.time["column"]}}}
            sources_dict[self.time["column"]]["date_histogram"]["interval"] = self.time["interval"]
            sources_dict[self.time["column"]]["date_histogram"]["format"] = "yyyy-MM-dd HH:mm:ss"
            self.dsl_composite.append(sources_dict)
        if len(self.dsl_composite) == 0:
            del self.dsl["aggs"]
        else:
            self.dsl["size"] = 0
            self.dsl['aggs']['groupby']['composite']['size'] = self.limit
            if self.time["interval"] is None or self.time["interval"] == "":
                raise Exception("time字段必须指定interval")

    def parse(self):
        """
        :return: 完整的DSL语句
        """
        self.dsl["size"] = self.limit
        self._parse_where()
        self._parse_composite()
        self._parse_fields()
        self._parse_metric()
        #print(self.dsl)
        return self.dsl


