# encoding=utf-8
from elasticsearch import Elasticsearch
import psycopg2
from pydruid.db import connect

__all__ = ['EnumDataBase', 'DataBase', 'db_configs']


# 枚举类
class EnumDataBase:
    PG = 'PostgreSQL'
    MYSQL = 'MySql'
    ES = 'Elasticsearch'
    DRUID = 'Druid'


class DataBase:
    """
    DataBase封装各个数据源进行连接的信息，含有各个数据源的表信息，以及数据库连接逻辑.
    通过该类的db属性可以直接拿到一个数据库的连接对象。查询中无需进行连接操作
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
        self.db_name = db_name
        self.db_type = db_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
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

    def connect_db(self):
        if self.db_type == EnumDataBase.PG:
            if self.db_name is None or self.port is None or self.user is None or self.password is None:
                raise RuntimeError("Empty elements in PgSQL")
            return psycopg2.connect(database=self.db_name, user=self.user,
                                    password=self.password, host=self.host, port=self.port)
        elif self.db_type == EnumDataBase.MYSQL:
            if self.db_name is None or self.port is None or self.user is None or self.password is None:
                raise RuntimeError("Empty elements in PgSQL")
        elif self.db_type == EnumDataBase.DRUID:
            if self.host is None or self.port is None:
                raise RuntimeError("Empty elements in Druid")
            return connect(host=self.host, port=self.port)
        elif self.db_type == EnumDataBase.ES:
            return Elasticsearch(self.host + ":" + str(self.port))


"""
    该单例类主要存放DataBase对象。
    字典结构: {db_name:dataBase对象}  db_name为数据源名称，主要是为了区分不同的数据源。非实际数据库名称
    类中支持对数据库对象的CRUD
"""


class _DbConfigs:
    def __init__(self):
        self.configs = {}

    def get_dbname_list(self):
        """
        :return: 返回所有Database类的名字
        """
        return self.configs.keys()

    def put(self, *Database_tuple):
        """
        批量添加Database对象
        :param Database_tuple: Database的可变参数列表
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
        self.configs[name] = DataBase(name, db_type, host, db_name, port, user, password, schema, tables,
                                      black_tables, black_fields)

    def remove_by_dbname(self, dbname):
        if name in self.configs:
            del self.configs[dbname]


db_configs = _DbConfigs()  # 单例模式
