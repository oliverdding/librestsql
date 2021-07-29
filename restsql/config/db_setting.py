# encoding=utf-8
from  elasticsearch import Elasticsearch
__all__=['EnumDataBase','DataBase','db_configs']
class EnumDataBase:
    PG = 'PostgreSQL'
    MYSQL = 'MySql'
    ES = 'Elasticsearch'
    DRUID='Druid'


class DataBase:
    """
    DataBase封装各个数据源进行连接的信息，含有各个数据源的表信息，以及数据库连接逻辑


    Database初始化器。
    :param name: 该Database的name。用于区分Database.
    :param db_type: 数据库类型。由EnumDataBase枚举类定义。
    :param host: 数据库host。
    :param db_name: 数据库名。
    :param port: 端口名。
    :param user: 用户名。用于连接数据库。
    :param password: 密码。用户连接数据库。
    :param schema: 模式。用于pgsql数据源。
    :param tables: 表。用户自定义相关表。是继承自Table类的类的list。
    :param black_tables: 黑名单表。使用自动维护时有用。是string的list。
    :param black_fields: 黑名单字段。使用自动维护时有用。是字典，结构为{'表名': ['需忽视字段名', ], }





    """
    def __init__(self, name, db_type, host, db_name=None, port=None, user=None, password=None, schema=None,
                 tables=None, black_tables=None, black_fields=None):
        if tables is None:
            tables = []
        if black_fields is None:
            black_fields = {}
        if black_tables is None:
            black_tables = []
        self.name = name
        # self.db_name = db_name
        self.db_type = db_type
        # self.host = host
        # self.port = port
        # self.user = user
        # self.password = password
        self.schema = schema
        if isinstance(tables, list):
            self.tables = tables
        else:
            raise RuntimeError("List of class(extended from Table) needed.")
        if isinstance(black_tables, list):
            self.black_tables = black_tables
        else:
            raise RuntimeError("List of string(tables' name) needed.")
        if isinstance(black_fields, dict):
            self.black_fields = black_fields
        else:
            raise RuntimeError("Dict of table_name: [field_name] needed.")

        if db_type == EnumDataBase.PG:
            if db_name is None or port is None or user is None or password is None:
                raise RuntimeError("Empty elements in PgSQL")
            """
            针对sql数据源，传入数据源对象（ORM或自定义数据库类）
            """
        elif db_type == EnumDataBase.MYSQL:
            if db_name is None or port is None or user is None or password is None:
                raise RuntimeError("Empty elements in PgSQL")
            """
            针对sql数据源，传入数据源对象（ORM或自定义数据库类
            """
        elif db_type == EnumDataBase.ES:
            self.db = Elasticsearch(host+":"+str(port))

# 该类作为保存数据库配置对象，作为单例模式，仅在项目程序驱动时候实现对象，对yaml文件进行读取一次
class _DBConfigs:
    def __init__(self):
        self.configs ={}

    def get_dbname_list(self):
        """
        :return: 返回所有Database类的名字
        """
        return self.configs['db_settings'].keys()

    def put(self, *Database_tuple):
        """
        批量添加Database对象
        :param Database_tuple): Database的可变参数列表
        :return: None
        """
        for db in Database_tuple:
            if not isinstance(db, DataBase):
                raise RuntimeError("DataBase needed!")
            self.configs[db.name] = db
    def get_by_dbname(self, dbname):
        if dbname in self.configs:
            return self.configs[dbname]

    def add_db(self, name, db_type, host, db_name=None, port=None, user=None, password=None, schema=None,
            tables=None, black_tables=None, black_fields=None):
        # 后期这里判断空处理
        self.configs[name] = DbSetting(name, db_type, host, db_name, port, user, password, schema, tables,
                                            black_tables, black_fields)

    def remove_by_dbname(self,dbname):
        if name in self.configs:
            del self.configs[dbname]

db_configs = _DBConfigs()  # 单例模式
