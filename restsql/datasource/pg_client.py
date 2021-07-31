# encoding=utf-8

import pandas as pd
import psycopg2

from restsql.config.database import DataBase
from restsql.config.model import Client,Query



def _build_select(select, time):
    """
    完成SELECT这一部分的SQL代码转化
    :param select: 所需查询的所有字段的字典
    :param time: 包含时序处理信息的字典
    :return: SELECT这一部分的SQL代码
    """
    # 将每次得到的部分sql语句放在一个列表种，最后调用join连接在一起，避免浪费内存
    sql_list = []
    s_sql = 'SELECT floor(extract(epoch from {column})/{interval})*{interval} AS time ' \
        .format(column=time['column'], interval=time['interval'])
    sql_list.append(s_sql)
    if len(select) != 0:
        sql_list.append(',')
    for s in select:
        # 判断是否使用聚合函数
        if len(s['metric']) > 0:
            sql_list.append(
                '{metric}({column}) '.format(metric=s['metric'], column=s['column'])
            )
        else:
            sql_list.append(
                '{column} '.format(column=s['column'])
            )
        # 判断是否有别名
        if len(s['alias']) > 0:
            sql_list.append(
                'AS {alias} '.format(alias=s['alias'])
            )
        sql_list.append(',')
    return ''.join(sql_list)[:-1]  # 连接select这部分的完整sql，且去掉末尾多的一个逗号后返回


def _build_filter(filters, time):
    """
    完成WHERE这一部分的SQL代码转化
    :param filters: 所需过滤条件的列表
    :param time: 包含时序处理信息的字典
    :return: WHERE这一部分的SQL代码
    """
    # 将每次得到的部分sql语句放在一个列表种，最后调用join连接在一起，避免浪费内存
    filter_list = ["{time}>='{begin}'".format(time=time['column'], begin=time['begin']),
                   "{time}<='{end}'".format(time=time['column'], end=time['end'])]
    for f in filters:
        filter_list.append(
            "{column}{op}'{value}'".format(column=f['column'], op=f['op'], value=f['value'])
        )
    return 'WHERE {filter}'.format(filter=' AND '.join(filter_list))


def _to_sql(que: Query):
    """
    把需要查询的内容转化为普通的SQL语句
    :param que: 包含查询所需信息的Query对象
    :return: 常规的SQL语句
    """
    source = 'from ' + que.From.split('.')[1] + ' '  # 数据源
    select = _build_select(que.select_list, que.time_dict)  # 字段这部分sql
    filters = _build_filter(que.where_list, que.time_dict)  # 过滤条件
    groups = 'GROUP BY ' + 'time' + ','.join(que.group_list) + ' '  # 分组
    limit = 'limit ' + str(que.limit)  # 数据量
    sql = select + source + filters + groups + limit
    return sql


def _query(database: DataBase, que: Query):
    """
    根据查询对象que里面的信息向database这个数据库发起查询请求，结果以DataFrame格式返回
    :param database: 数据库配置类
    :param que: 包含查询所需信息的Query对象
    :return: DataFrame格式的返回数据
    """
    conn = psycopg2.connect(database=database.db_name, user=database.user,
                            password=database.password, host=database.host, port=database.port)
    sql = _to_sql(que)  # 转为普通sql语句
    columns = ['time']  # 默认把时间添加到第一个列
    # 获取返回结果的名字（字段名或别名）
    for i in que.select_list:
        c = i['column']
        if len(i['alias']) > 0:
            c = i['alias']
        columns.append(c)
    # 调用数据库，得到sql语句查询的结果
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    except psycopg2.Error as e:
        print(e)
        return pd.DataFrame()   # 若查询过程中出现错误，打印错误类型，返回空的DataFrame
    # 以dataFrame格式返回
    res = pd.DataFrame(data=rows, columns=columns)
    return res


class PgClient(Client):
    """
    提供给外界的接口类
    """
    def query(self, query: Query):
        return _query(self.dataBase, query)
