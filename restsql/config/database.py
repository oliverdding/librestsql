from elasticsearch import Elasticsearch
from pydruid.db import connect
from peewee import PostgresqlDatabase
import os
import sys
import psycopg2

__all__ = ['EnumDataBase', 'DataBase', 'db_settings']
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)


class EnumDataBase:
    PG = 'PostgreSQL'
    ES = 'Elasticsearch'
    DRUID = 'Druid'


class DataBase:
    """
    db_setting类。存储各个数据源的配置信息，用于连接、peewee查询。这里应该用builder模式但是py下的构造着模式有些奇怪，就改动了下。
    """

    def __init__(self, name, db_type, host, db_name=None, port=None, user=None, password=None, schema=None,
                 tables=None, black_tables=None, black_fields=None):
        """
        db_setting初始化器。
        :param name: 该配置数据源的名称
        :param db_type: 数据库类型。由EnumDataBase枚举类定义。
        :param host: 数据库host。
        :param db_name: 数据库名。
        :param port: 端口名。
        :param user: 用户名。用于连接数据库。
        :param password: 密码。用户连接数据库。
        :param schema: 模式。用于pgsql数据源。
        :param tables: 表。作为list对象。
        :param black_tables: 黑名单表。是string的list。
        :param black_fields: 黑名单字段。
        """
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
        elif self.db_type == EnumDataBase.DRUID:
            if self.host is None or self.port is None:
                raise RuntimeError("Empty elements in Druid")
            return connect(host=self.host, port=self.port)
        elif self.db_type == EnumDataBase.ES:
            return Elasticsearch(self.host + ":" + str(self.port))


class _DbSettings:

    def __init__(self):
        self._db_settings = {}

    def get_all_name(self):
        """
        获取当前db_settings中所有db_setting的name的list
        :return: List of db_settings' name
        """
        return self._db_settings.keys()

    def put(self, *db_setting_tuple):
        """
        向db_settings中直接添加db_setting实例
        :param db_setting_tuple: db_setting的可变参数列表
        :return: None
        """
        for db_setting in db_setting_tuple:
            if not isinstance(db_setting, DataBase):
                raise RuntimeError("DbSetting needed!")
            self._db_settings[db_setting.name] = db_setting

    def add(self, name, db_type, host, db_name=None, port=None, user=None, password=None, schema=None,
            tables=None, black_tables=None, black_fields=None):
        """
        向db_settings中添加新的db_setting类。
        :param name: 该db_setting的name。用于区分db_setting.
        :param db_type: 数据库类型。由EnumDataBase枚举类定义。
        :param host: 数据库host。
        :param db_name: 数据库名。
        :param port: 端口名。
        :param user: 用户名。用于连接数据库。
        :param password: 密码。用户连接数据库。
        :param schema: 模式。用于pgsql数据源。
        :param tables: 表。用户自定义相关表。是继承自Table类的类的list。
        :param black_tables: 黑名单表。是string的list。
        :param black_fields: 黑名单字段。是字典，结构为{'表名': ['字段名', ], }
        :return: None
        """
        self._db_settings[name] = DataBase(name, db_type, host, db_name, port, user, password, schema, tables,
                                           black_tables, black_fields)

    def get_by_name(self, name):
        """
        :param name: 待获取db_setting的name
        :return: db_setting
        """
        if name in self._db_settings:
            return self._db_settings[name]


#  以单例的方式生成db_settings
db_settings = _DbSettings()
