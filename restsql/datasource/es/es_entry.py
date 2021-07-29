import pandas as pd


# {
#     "db_setting":{
#         'name':'前端配置数据源指定的名字，此处不用管'
#         "usernmae": 'root'
#         "password": 'xxxxx'
#         "host":'192.168.x.x'
#         'port':'3306'
#         'schema': 'xxx' //如 postgre里面的public
#         'type': 'xxx' //es 或 sql
#     }
# }


class Query:
    def __init__(self, query, database,pid):
        self.query = query
        self.database = database
        self.pid = pid
        self._result = None
        # self.table=self._get_table() 如果有orm需要指定对象,进行使用

    # def _get_table(self): 有orm需要生成model，使用

    def es_do_query(self):
        a = self.query  # 有需要的参数调用构造器里面赋值的东西就行

        # xxx操作
        self._result = "this is a sql result"
        # 调用self.query进行操作 ,然后把字符串结果进行存储到  _result里面进去

    @property  # 调用方法名，不用加()
    def result(self):
        return self._result


class EsClient:
    def __init__(self, datasource):
        self.datasource = datasource
        print("init es client")

    # 这里为暴露的接口，供进行调用！！！  统一返回dateframe
    def es_query(self,querysql, pid):  # 数据源是固定的，但是table可能有变化
        query = Query(querysql, self.datasource,pid)
        query.es_do_query()

        # result= query.result #获取对象
        # 进行额外处理,如果结果含有时间戳， 把时间戳换成毫秒那个格式，然后对应的具体时间点先指定为聚合时间段的中点
        data = [['Alex', 10], ['Bob', 12], ['Clarke', 13]]
        return pd.DataFrame(data, columns=['Name', 'Age'], dtype=float)  # 举例 返回样式
        # 统一返回dataframe
