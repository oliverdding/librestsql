from restsql.config.database import DataBase
from restsql.query import *
import re

__all__ = ['check']


def _check_op(op):
    """
    检查操作符op是否支持
    :param op:
    :return:
    """
    # 合法的操作符
    legal_op = ['=', '>', '>=', '<', '<=', 'in', 'IN', 'NOT IN', 'not in', 'LIKE', 'like',
                'startswith', 'endswith', 'contains']
    if op not in legal_op:
        raise RuntimeError('"{op}" op is not supported'.format(op=op))


def _check_metric(metric):
    """
    检查聚合函数metric是否支持
    :param metric: 聚合函数名
    :return:
    """
    legal_metric = ['', 'SUM', 'sum', 'AVG', 'avg', 'COUNT', 'count', 'MAX', 'max'
                                                                             'MIN', 'min', 'COUNT DISTINCT',
                    'count distinct']
    if metric not in legal_metric:
        raise RuntimeError('"{metric}" metric is not supported'.format(metric=metric))


def _check_column(column):
    """
    检查表名或字段名是否符合规范，只支持中文，大小写字母，数字，下划线
    :param column: 字段名
    :return:
    """
    if re.match(pattern=r'^[\u4E00-\u9FA5A-Za-z0-9_]+$', string=column) is None:
        raise RuntimeError('Field "{}" error'.format(column))


def _check_blacklist(que: Query, database: DataBase):
    """
    检查所需查询的表和字段是否在黑名单里
    :param que:
    :param database:
    :return:
    """
    table = que.target.split('.')[1]
    if table in database.black_tables:
        raise RuntimeError('Table "{}" access denied'.format(table))
    if table in database.black_fields.keys():
        for c in que.select_list:
            if c['column'] in database.black_fields[table]:
                raise RuntimeError('Field "{}" access denied'.format(c['column']))


def check(que: Query, database: DataBase):
    """
    检查表名以及所有字段名是否符合规范（只支持中文，大小写字母，数字，下划线）
    检查操作符op以及聚合函数metric是否支持,以及访问的字段和表在不在黑名单里
    :param database:
    :param que: 包含查询信息的Query封装对象
    :return:
    """
    # 检查字段以及表名是否在黑名单内
    _check_blacklist(que, database)
    # 检查表名是否符合规范
    _check_column(que.target.split('.')[1])
    # 检查SELECT中是否有非法字符
    for s in que.select_list:
        # 遍历每个select字典
        for k, v in s.items():
            if k == 'metric':
                _check_metric(v)
            if k == 'column':
                _check_column(v)

    # 检查WHERE中字段是否符合规范
    for f in que.where_list:
        # 遍历每个where字典
        for k, v in f.items():
            if k == 'op':
                _check_op(v)
            if k == 'column':
                _check_column(v)
    # 检查GROUP中是否有非法字符
    for g in que.group_list:
        _check_column(g)
