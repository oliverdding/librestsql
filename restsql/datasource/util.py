from restsql.config.settings import EnumDataBase
from restsql.query import Query


def _build_select(select, time, sql_type):
    """
    完成SELECT这一部分的SQL代码转化
    :param select: 所需查询的所有字段的字典
    :param time: 包含时序处理信息的字典
    :return: SELECT这一部分的SQL代码
    """
    # 将每次得到的部分sql语句放在一个列表种，最后调用join连接在一起，避免浪费内存
    sql_list = []
    # 判断时间这一字段是否设置
    if len(time['column']) > 0:
        time_select_sql = ''  # 时间这一字段的SELECT的SQL语句
        # 判断属于什么类型的SQL（比如Druid和Postgre在时间处理上有些许不同）
        if sql_type == EnumDataBase.DRUID:
            time_select_sql = "SELECT TIME_FLOOR({column}, 'PT{interval}S') AS \"time\"" \
                                .format(column=time['column'], interval=time['interval'])
        elif sql_type == EnumDataBase.PG:
            time_select_sql = 'SELECT floor(extract(epoch from {column})/{interval})*{interval} AS "time" ' \
                                .format(column=time['column'], interval=time['interval'])
        sql_list.append(time_select_sql)
        # 当存在时序字段，且还有其他需要查询的字段，添加逗号
        if len(select) > 0:
            sql_list.append(',')
    # 当既不存在时序字段也没有任何需要查询的字段时报错
    elif len(select) == 0:
        raise RuntimeError('SELECT is empty!')
    # 无时序字段，有其他需要查询的字段（即普通的表查询）
    elif len(select) > 0:
        sql_list.append('SELECT ')
    # 遍历生成所有字段的SQL代码
    for s in select:
        # 判断是否使用聚合函数
        if len(s['metric']) > 0:
            # count distinct使用格式较为特殊，单独处理
            if s['metric'] in ['COUNT DISTINCT', 'count distinct']:
                sql_list.append('COUNT(DISTINCT {column}) '.format(column=s['column']))
            else:
                sql_list.append('{metric}({column}) '.format(metric=s['metric'], column=s['column']))
        else:
            sql_list.append('{column} '.format(column=s['column']))
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
    # 若没有任何过滤条件（包括时间）
    if len(time['column']) == 0 and len(filters) == 0:
        return ''
    # 将每次得到的部分sql语句放在一个列表种，最后调用join连接在一起，避免浪费内存
    filter_list = []
    # 若存在时序字段，则将此添加到第一个filter（当无时序字段的时候，相当于普通的表查询）
    if len(time['column']) > 0:
        filter_list.append("{time}>='{begin}' ".format(time=time['column'], begin=time['begin']))
        filter_list.append("{time}<='{end}' ".format(time=time['column'], end=time['end']))
    for f in filters:
        filter_list.append(
            "{column}{op}'{value}' ".format(column=f['column'], op=f['op'], value=f['value'])
        )
    return 'WHERE {filter}'.format(filter=' AND '.join(filter_list))


def _build_group(group_list, time, sql_type):
    """
    完成GROUP这一部分SQL代码转化
    :param group_list: 需要分组的字段名列表
    :param time: 包含时序处理信息的字典
    :param sql_type: 需要生成的Sql类型，比如Druid或Postgresql
    :return: GROUP这一部分的SQL代码
    """
    # 若既没时序字段也没任何需要分组的字段，直接返回空字符串
    if len(time['column']) == 0 and len(group_list) == 0:
        return ''
    res_list = []
    # 判断sql类型（在group处理上pg和druid有不同，druid需要加单引号，pg不需要）
    if sql_type == EnumDataBase.DRUID:
        # 若存在时序字段，则置首位为1
        if len(time['column']) > 0:
            res_list.append('1')
        res_list.extend(["'{}'".format(i) for i in group_list])
    elif sql_type == EnumDataBase.PG:
        # 若存在时序字段，则将时序字段放在第一个GROUP
        if len(time['column']) > 0:
            res_list.append('time')
        res_list.extend(group_list)
    return 'GROUP BY {} '.format(','.join(res_list))


def to_sql(que: Query, sql_type):
    """
    把需要查询的内容转化为普通的SQL语句
    :param sql_type: 需要生成的Sql类型，比如Druid或Postgresql
    :param que: 包含查询所需信息的Query对象
    :return: 常规的SQL语句
    """
    _check_field(que)  # 检查是否含有非法字段
    source = 'from ' + que.From.split('.')[1] + ' '  # 数据源
    select = _build_select(que.select_list, que.time_dict, sql_type)  # 字段这部分sql
    filters = _build_filter(que.where_list, que.time_dict)  # 过滤条件
    group = _build_group(que.group_list, que.time_dict, sql_type)  # 分组
    limit = 'limit ' + str(que.limit)  # 数据量
    sql = select + source + filters + group + limit  # 最终的sql拼接
    return sql


def get_columns(que: Query):
    """
    获取需要查询的所有字段名
    :param que: 包含查询信息的Query封装对象
    :return: 所有字段名构成的列表
    """
    columns = []  # 默认把时间添加到第一个列
    if len(que.time_dict['column']) > 0:
        columns.append('time')
    # 获取返回结果的名字（字段名或别名）
    for i in que.select_list:
        c = i['column']
        if len(i['alias']) > 0:
            c = i['alias']
        columns.append(c)
    return columns


def _is_illegal(value):
    """
    检查value中是否还有illegal_char里面定义的非法字符
    :param value: 需要检查的值
    :return:
    """
    illegal_char = ['--', ' ', ';', '/*', '"', "'"]  # 非法字符
    for ill in illegal_char:
        if ill in value:
            raise RuntimeError('Illegal characters "{ill}" found'.format(ill=ill))


def _check_field(que: Query):
    """
    检查输入的信息中是否有非法字符，例如 '-',';'以及空格，防止SQL注入
    :param que: 包含查询信息的Query封装对象
    :return:
    """
    # 检查SELECT中是否有非法字符
    for s in que.select_list:
        # 遍历每个select字典
        for k, v in s.items():
            if k == 'metric':
                # 若通过了聚合函数名检查则该字段必然合法，跳过合法检验
                if _check_metric(v):
                    continue
            _is_illegal(v)
    # 检查WHERE中是否有非法字符
    for f in que.where_list:
        # 遍历每个where字典
        for k, v in f.items():
            if k == 'op':
                # 若通过了操作符检查则该字段必然合法，跳过合法检验
                if _check_op(v):
                    continue
            _is_illegal(v)
    # 检查GROUP中是否有非法字符
    for g in que.group_list:
        _is_illegal(g)


def _check_op(op):
    """
    检查操作符op是否支持
    :param op:
    :return: 若支持此操作符则返回True，否则抛出错误
    """
    legal_op = ['=', '>', '>=', '<', '<=', 'in', 'IN', 'NOT IN', 'not in', 'LIKE', 'like']
    if op not in legal_op:
        raise RuntimeError('"{op}" op is not supported'.format(op=op))
    return True


def _check_metric(metric):
    """
    检查聚合函数metric是否支持
    :param metric: 聚合函数名
    :return:
    """
    legal_metric = ['', 'SUM', 'sum', 'AVG', 'avg', 'COUNT', 'count', 'MAX', 'max'
                    'MIN', 'min', 'COUNT DISTINCT', 'count distinct']
    if metric not in legal_metric:
        raise RuntimeError('"{metric}" metric is not supported'.format(metric=metric))
    return True
