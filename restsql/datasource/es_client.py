from druid_client import Client
import pandas as pd

_all_ = ['EsClient']


class EsQuery:
    """
    将请求协议转译为Es—DSL查询语句
    """

    def __init__(self, query_dict):
        """
        :param query_dict: 请求协议的字典
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
        if "limit" in query_dict:
            self.limit = query_dict["limit"]
        self.index = query_dict["from"].split(".")[1]
        self.select_list = query_dict["select"]
        self.time = query_dict["time"]
        self.groupby_list = query_dict["group"]
        self.where_list = query_dict["where"]
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
            }
        }
        self.dsl_where = self.dsl['query']['bool']['must']
        self.dsl_composite = self.dsl['aggs']['groupby']['composite']['sources']
        self.dsl_aggs = self.dsl['aggs']['groupby']['aggs']

    def _parse_fields(self):
        if self.time["interval"] is None or self.time["interval"] == "":
            if len(self.groupby_list) == 0:
                self.dsl['_source']['includes'].append(self.time["column"])
        for s in self.select_list:
            self.dsl['_source']['includes'].append(s["column"])

    def _parse_where(self):
        """
        过滤操作暂时仅支持了and
        后续商讨后进行修改
        :return:
        """
        for filter in self.where_list:
            if filter["op"] == "=":
                self.dsl_where.append({
                    'term': {
                        filter["column"]: filter["value"]
                    }
                })
            elif filter["op"] == "<":
                self.dsl_where.append({
                    'range': {
                        filter["column"]: {'lt': filter["value"]}
                    }
                })
            elif filter["op"] == ">":
                self.dsl_where.append({
                    'range': {
                        filter["column"]: {'gt': filter["value"]}
                    }
                })
            elif filter["op"] == "<=":
                self.dsl_where.append({
                    'range': {
                        filter["column"]: {'lte': filter["value"]}
                    }
                })
            elif filter["op"] == ">=":
                self.dsl_where.append({
                    'range': {
                        filter["column"]: {'gte': filter["value"]}
                    }
                })
            else:
                raise SyntaxError('cat not support op: {0}, field: {1}'.format(filter["op"], filter["column"]))
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

    def _parse_groupby(self):
        func_map = {'count': 'value_count', 'sum': 'sum', 'avg': 'avg', 'max': 'max', 'min': 'min',
                    'count_distinct': 'cardinality'}
        for s in self.select_list:
            if s["metric"] in func_map.keys():
                # 针对文本数据进行聚合可能存在问题
                self.dsl_aggs[s["alias"]] = {func_map[s["metric"]]: {'field': s["column"]}}
            else:
                if s["metric"] == "" or s["metric"] is None:
                    continue
                raise SyntaxError('cat not support aggregation operation: {}'.format(s["metric"]))
        pass

    def _parse_composite(self):
        for g in self.groupby_list:
            sources_dict = {}
            sources_dict[g] = {"terms": {"field": g + ".keyword"}}
            self.dsl_composite.append(sources_dict)
        if self.time["interval"] is not None and self.time["interval"] != "":
            sources_dict = {}
            sources_dict[self.time["column"]] = {"date_histogram": {"field": self.time["column"]}}
            sources_dict[self.time["column"]]["date_histogram"]["interval"] = self.time["interval"]
            sources_dict[self.time["column"]]["date_histogram"]["format"] = "yyyy-MM-dd hh:mm:ss"
            self.dsl_composite.append(sources_dict)
        if len(self.dsl_composite) == 0:
            del self.dsl["aggs"]

    def parse(self):
        """
        :return: 完整的DSL语句
        """
        self.dsl["size"] = self.limit
        self._parse_where()
        self._parse_composite()
        self._parse_fields()
        self._parse_groupby()
        return self.dsl


class EsClient(Client):
    """
    Es数据源服务类，供restSqlClient调用
    """

    def __init__(self, database):
        super().__init__(database)
        print("init es client")

    # 这里为暴露的接口，供进行调用！！！  统一返回dateframe
    def query(self, query_dict):
        """
        :param query_dict: 请求协议字典
        :return: 返回DataFrame
        """
        alias_dict = {}
        for s in query_dict["select"]:
            alias_dict[s["column"]] = s["alias"]
        alias_dict[query_dict["time"]["column"]] = "time"
        results = []
        esQuery = EsQuery(query_dict)
        index = query_dict["from"].split(".")[1]
        dsl = esQuery.parse()
        raw_result = self.dataBase.db.search(index=index, body=dsl)
        if 'aggs' in raw_result or 'aggregations' in raw_result:
            if raw_result.get('aggregations'):
                results = raw_result['aggregations']['groupby']['buckets']
            else:
                results = raw_result['agg']['groupby']['buckets']
            for it in results:
                it["time"] = it["key"][query_dict["time"]["column"]]
                del it['key']
                del it['doc_count']  # TODO: 暂时没用的一个字段
                for field, value in it.items():
                    if isinstance(value, dict) and 'value' in value:
                        it[field] = value['value']
        elif 'hits' in raw_result and 'hits' in raw_result['hits']:
            for it in raw_result['hits']['hits']:
                record = it['_source']
                result = {}
                for field in record.keys():
                    if alias_dict[field] == "":
                        result[field] = record[field]
                    else:
                        result[alias_dict[field]] = record[field]
                results.append(result)
        return (pd.DataFrame(results))
