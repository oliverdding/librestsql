import yaml
from restsql.config.db_setting import db_configs

"""
{
    "db_setting":{
        'name':'前端配置数据源指定的名字，此处不用管'
        "usernmae": 'root'
        "password": 'xxxxx'
        "host":'192.168.x.x'
        'port':'3306'
        'schema': 'xxx' //如 postgre里面的public
        'type': 'xxx' //es 或 sql
    }
}
{
    name:{
        table1: rtable1
        table2: rtable2
    }
}
"""

# 如果考虑到ORM
"""
{
    rtable1:{
        username: xxx
        password: xxx--->指代是什么类型，字符串还是数字，方便使用field进行指定
    }
}
"""


# table 表名真实映射表，在rest协议里面提取from后半部分时，先查询一下，如果有映射，替换为真实的，否则不用,注意后期错误处理
def init_db_config():
    config = None
    with open("restsql.yaml", 'r') as fp:
        configjson = yaml.load(fp, Loader=yaml.FullLoader)
    print(configjson)
    dbs = configjson.get('db', {})
    dbs_name = dbs.keys()
    # 构成一个表名映射表
    temp_tables = configjson.get('table', {})

    for db_name in dbs_name:
        table = {}
        db_configs.config['tables'][db_name] = {}
        temp_table_names = temp_tables.get(db_name, {})
        for temp_table_name in temp_table_names:
            # 此处可以加上错误处理
            table[temp_table_name] = temp_tables[db_name][temp_table_name]
        db_configs.addtable(db_name, table)
    # 如果不需要进行错误处理，可以直接  tables=configjson.get('table')

    # 下面可以考虑是是不是还有表结构指定什么的可以

    # 构成一个db配置json
    for name in dbs_name:
        values = dbs.get(name, {})
        temp_dict = db_configs.param_format(name,
                                            values.get('dbname', None),
                                            values.get('schema', None),
                                            values.get('host', None),
                                            values.get('port', None),
                                            values.get('username', None),
                                            values.get('password', None),
                                            values.get('type', None))
        db_configs.adddb(name, temp_dict)
    fp.close()


def add_db_setting(name, dbname, schema, host, port, username, password, type):
    with open("restsql.yaml", 'r') as fp:
        configjson = yaml.load(fp, Loader=yaml.FullLoader)
        print(configjson)
    fp.close()
    temp = db_configs.param_format(dbname, schema, host, port, username, password, type)
    configjson['db'][name] = temp

    # 写入到yaml文件
    with open("restsql1.yaml", "w", encoding="utf-8") as f:
        yaml.dump(configjson, f)
    f.close()
