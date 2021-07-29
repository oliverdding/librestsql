# 创单数据库对象
# 对象： db_settings 为每一个数据库源配置， tables 为每一个数据库源当中是否还要而外指定表名映射  tables_structs 为sql orm所进行动态类型配置（es如不需要可忽略）
# {
#     db_settings{
#         'name1':{
#             "name":name1,
#             "dbname": dbname,
#             "schema":schema,
#             "host": host,
#             "port": port,
#             "username": username,
#             "password": password,
#             "type": type
#             "blacktables":[]
#         }
#         'name2':{
#
#         }
#     }
#     tables:{
#         'name1':
#         'name2':
#        }
#     tables_struct:{
#         "name1":{
#             "table1":{
#                 "column1": "String"
#                 "column2": "Integer"
#                 "column3": "Timestamp"
#             }
#             "table2":{
#                 "column1": "xxx"
#                 "column2": "xxx"
#             }
#         }
#         "name2":{
#
#         }
#     }
#     "blackcolumns":{
#           "table1":[
#
#           ]
#      }
#
# }

# 该类作为保存数据库配置对象，作为单例模式，仅在项目程序驱动时候实现对象，对yaml文件进行读取一次
class _DBConfig:
    def __init__(self):
        self.config = {'db_settings': {}, 'tables': {}, 'tables_struct': {}, 'columns_black': {}}

    def get_db_list(self):
        return self.config['db_settings'].keys()

    def get_db_bydbname(self, dbname):
        dbs = self.config.get('db_settings', {})
        return dbs.get(dbname, {})

    def add_db(self, name, tempdict):
        # 后期这里判断空处理
        self.config['db_settings'][name] = tempdict

    def add_table(self, name, table):
        self.config['tables'][name] = table

    def get_tables_list(self):
        return self.config['tables']

    def get_table_byname(self, name):
        return self.config['tables'].get(name, {})

    def get_tablestruct_byname(self, dbname, tablename):
        tables = self.config['tables_struct'].get(dbname, None)
        if tables:
            return tables.get(tablename, {})
        else:
            return {}

    def add_tablestruct(self, dbname, tablename, struct):
        tables = self.config['tables_struct'].get(dbname, None)
        if not tables:
            self.config['tables_struct'][dbname] = {}

        self.config['tables_struct'][dbname][tablename] = struct

    def get_tableblack_byname(self, dbname):
        db_setting = self.get_db_bydbname(dbname)
        if db_setting:
            return db_setting.get('blacktables', [])
        else:
            return []

    def add_tableblack(self, name, blacktables):
        db_setting = self.get_db_bydbname(name)
        if db_setting:
            db_setting['blacktables'] = blacktables

    def get_blackcolumn_bytablename(self,dbname,tablename):
        tables = self.config['columns_black'].get(dbname, None)
        if tables:
            return tables.get(tablename, [])
        return []

    def add_blackcolumn(self,dbname, name, blackcolumns):
        tables = self.config['columns_black'].get(dbname, None)
        if not tables:
            self.config['columns_black'][dbname] = {}
        self.config['columns_black'][dbname][name] = blackcolumns

    def param_format(self, name, dbname, schema, host, port, username, password, sourcetype):
        tempdict = {}
        if name:
            tempdict['name'] = name
        if dbname:
            tempdict['dbname'] = dbname
        if schema:
            tempdict['schema'] = schema
        if host:
            tempdict['host'] = host
        if port:
            tempdict['port'] = port
        if username:
            tempdict['username'] = username
        if password:
            tempdict['password'] = password
        if sourcetype:
            tempdict['type'] = sourcetype
        return tempdict


db_configs = _DBConfig()  # 单例模式
