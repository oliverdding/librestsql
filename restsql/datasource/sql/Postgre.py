import pandas as pd
import json
import psycopg2


def select_handler(select, time):
    # select这部分sql语句
    if time:
        s_sql = "SELECT " + 'floor(extract(epoch from ' + \
                time['column'] + ')/' + time['interval'] + ')' + '*' + time['interval'] + ' AS time '
    else:
        s_sql = "SELECT "
    # if len(select) != 0:
    #     s_sql += ','
    #注意这里假如没有时间，有没有逗号！！！！
    for s in select:
        if len(s.get('metric',"")) > 0:
            s_sql += s['metric'] + '(' + s['column'] + ') '
        else:
            s_sql += s['column'] + ' '
        if len(s['alias']) > 0:
            s_sql += 'AS ' + s['alias'] + ' '
        s_sql += ','
    return s_sql[:len(s_sql) - 1]


def filter_handler(filters, time, p_id):
    # where这部分sql语句
    if time:
        f_sql = 'WHERE ' + time['column'] + '>=\'' + time['begin'] + '\' AND ' + time['column'] + '<=\'' + time[
            'end'] + '\''
        f_sql+=' AND '
    else:
        f_sql = 'WHERE '
    f_sql += 'p_id=' + p_id + ' '
    if len(filters) != 0:
        f_sql += 'AND '
    else:
        for i in range(0, len(filters), 2):
            f_sql += filters[i]['column'] + filters[i]['op'] + filters[i]['value'] + ' '
            if i != len(filters) - 1:
                f_sql += filters[i + 1]['op'] + ' '
    return f_sql


def to_sql(query_dic, tablename, p_id):
    source = 'from ' + tablename + ' '  # 数据源
    # select = select_handler(query_dic['select'], query_dic['time'])  # 字段这部分sql
    # filters = filter_handler(query_dic['where'], query_dic['time'], p_id)  # 过滤条件
    select = select_handler(query_dic['select'], None)  # 字段这部分sql
    filters = filter_handler(query_dic['where'], None, p_id)  # 过滤条件
    # groups = 'GROUP BY ' + 'time' + ','.join(query_dic['group']) + ' '  # 分组

    # groups = 'GROUP BY '  # 分组

    limit = 'limit ' + str(query_dic['limit'])  # 数据量
    # sql = select + source + filters + groups + limit #加入没有group by呢？？？
    sql = select + source + filters  + limit
    print(sql)
    return sql


def query(database, query_dic, p_id):
    db_setting = database['db_setting']
    print(db_setting['dbname'])
    print(db_setting['username'])
    print(db_setting['password'])
    print(db_setting['host'])
    print(db_setting['port'])
    conn = psycopg2.connect(database=db_setting['dbname'], user=db_setting['username'],
                            password=db_setting['password'], host=db_setting['host'], port=db_setting['port'])
    # conn = psycopg2.connect(database="testdjango", user="postgres", password="zhang23785491", host="127.0.0.1",
    #                         port="5432")
    tablename = database['table'][0]['tablename']  # 这里的tablename要在这里获取
    if tablename:
        sql = to_sql(query_dic, 'public."'+tablename+'"', '\''+p_id+'\'' ) # 将Json格式的restsql协议转为普通sql语句,  注意有一个Public,需要处理
    else:
        return False  # 举例万一错误的时候
    # columns = ['time']  # 默认把时间添加到第一个列
    columns=[]
    # 获取返回结果的名字（字段名或别名）
    for i in query_dic['select']:
        c = i['column']
        if len(i['alias']) > 0:
            c = i['alias']
        columns.append(c)
    # 调用数据库，得到sql语句查询的结果
    print("打印转义的sql语句")
    print(sql)
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    # 以dataFrame格式返回
    res = pd.DataFrame(data=rows, columns=columns)
    return res
