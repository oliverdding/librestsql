# encoding:utf-8

# 空行
# 空格
# 函数名混杂驼峰/下划线
# Python 规范文档
# SQL 拼接尽量避免，有 SQL 注入风险
# 测试使用单独的文件，不写在 main 里面
# 代码按时提交，推荐每天提交一次
# 主干开发，master 拉分支合master
# Django & librestsql 代码逻辑区分清楚，Django 可替换为其他 Web 框架



import json
import  time


class Engine():
    def parse(self):
        """
        :return: 返回数据库具体查询语句
        """
        raise NotImplementedError

class EsEngine(Engine):
    """
    主要采用SQL方式实现降低学习成本，无法实现的需求混合DSL查询
    """
    def __init__(self,query_dict):
        self.index=query_dict["from"].split(".")[1]
        self.select_list=query_dict["select"]
        self.time = query_dict["time"]
        self.groupby_list=query_dict["group"]
        self.where_list=query_dict["where"]
        pass
    def _parse_select(self):
        """
        :param select_list: 协议中“select”下的列表
                time_column:时间列
        :return: 查询的sql参数字符串
        """
        selectSQL="select " + self.time["column"] + " as time, "
        for index,col in enumerate(self.select_list):
            if col["metric"]=="":
                selectSQL+=col["column"]
            else:
                selectSQL+=(col["metric"]+"("+col["column"]+")")
            if index!=len(self.select_list)-1:
                selectSQL += (" as " + col["alias"] + ",")
            else:
                selectSQL += (" as " + col["alias"] + " ")

        return selectSQL


    def _parse_timefilter(self):
        """
        begin若为空，则为格林威治时间1970年01月01日00时00分00秒
        end若为空，则为当前timestamp
        :return:
        """
        timefilterSQL=""
        if self.time["begin"]!="":
            timefilterSQL += self.time["column"]+ " >= " + self.time["begin"] + " and "
        else:
            timefilterSQL += self.time["column"] + " >= " + "0"+ " and "
        if self.time["end"] != "":
            timefilterSQL += self.time["column"] + " <= " + self.time["end"]+" "
        else:
            timefilterSQL += self.time["column"] + " <= " + str(round(time.time()*1000))+" "

        return timefilterSQL
    def _parse_where(self):
        whereSQL=" "
        for filter in self.where_list:
            if filter["column"]!="":
                whereSQL+=filter["column"]+filter["op"]+filter["value"]+" "
            else:
                whereSQL+=filter["op"]+" "
        return whereSQL
    def _parse_group(self):
        if len(self.groupby_list)==0:
            return ""
        group_sql=" group by "
        for index,g in enumerate(self.groupby_list):
            if index!= len(self.groupby_list)-1:
                group_sql += g+","
            else:
                group_sql+=g
        return group_sql
    def _parse_timebucket(self):
        """
        商讨interval可选值1s,1m,1h,1d,1m,1y
        :return:
        """
        if self.time["interval"]=="":
            return ""
        if len(self.groupby_list)==0:
            timebucketSQL=" group by "
        else:
            timebucketSQL=" ,"
        function_list=["YEAR","MONTH_OF_YEAR","DAY_OF_MONTH","HOUR_OF_DAY","MINUTE_OF_DAY","SECOND_OF_MINUTE"]
        param_list=["1y","1M","1d","1h","1m","1s"]
        for i in range(len(param_list)):
            if param_list[i]==self.time["interval"]:
                for j in range(i):
                    timebucketSQL+=function_list[j]+"("+self.time["column"]+")"+","
                timebucketSQL+=function_list[i]+"("+self.time["column"]+")"
        return timebucketSQL
        pass



    def parse(self):
        """
        :return: es-sql json格式查询
        """
        request_dict={}
        request_dict["query"]=self._parse_select()+" from "+self.index+" where "
        request_dict["query"]+=self._parse_timefilter()+" and "+self._parse_where()+self._parse_group()+self._parse_timebucket()
        return json.dumps(request_dict,ensure_ascii=False)
if __name__ == '__main__':
    #测试
    request={
            "from": "db.cars",
            "time": {
                "column": 'sold',
                "begin": "",
                "end": "",
                "interval": "1M"
            },
            "select": [
                {
                    "column": "color",
                    "alias": "coloralias",
                    "metric": ""
                },
                {
                    "column": "make",
                    "alias": "makealias",
                    "metric": ""
                }
            ],
            "where": [
                {
                    "column": "price",
                    "op": "<",
                    "value": "50000"
                },
                {
                    "column": "",
                    "op": "and",
                    "value": ""
                },
                {
                    "column": "price",
                    "op": ">",
                    "value": "20000"
                },
            ],
            "group": []
        }
    Es=EsEngine(request)
    print(Es.parse())

















