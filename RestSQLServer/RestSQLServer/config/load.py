# -*- coding:UTF-8 -*-
import json
import logging
import os
import yaml

from restsql.config.database import EnumDataBase, db_settings
from restsql.config.table import NumberField, StringField, BoolField, IntField, TimeField, Table

__all__ = ['init_yaml', 'CONF_RESTSQL_PATH']

logger = logging.getLogger("restsql_load")
curPath = os.path.dirname(os.path.realpath(__file__))
CONF_RESTSQL_PATH = os.getenv('CONF_RESTSQL_PATH', curPath + '/restsql.yml')  # 导入配置文件路径


# 注意dbsetting模块的导入的方式
def get_db_type(db_type):
    if db_type == "PG":
        return EnumDataBase.PG
    if db_type == "DRUID":
        return EnumDataBase.DRUID
    elif db_type == "ES":
        return EnumDataBase.ES
    else:
        logger.critical("载入数据源配置出错: 无法识别数据库类型: %s", db_type)
        raise Exception("载入数据源配置出错: 无法识别数据库类型: {}".format(db_type))


def init_yaml(path):
    with open(path, "r", encoding="utf-8") as fp:  # 配置文件由configmap挂载到镜像路径后传入
        config = yaml.safe_load(fp)
    temp_map = {}
    for table_config in config.get("tables", []):
        table_name = table_config.get("table_name")
        fields = {}
        for (k, v) in table_config.get("fields").items():
            if v == "IntField":
                fields[k] = IntField()
            elif v == "StringField":
                fields[k] = StringField()
            elif v == "NumberField":
                fields[k] = NumberField()
            elif v == "BoolField":
                fields[k] = BoolField()
            elif v == "TimeField":
                fields[k] = TimeField()
            else:
                logger.critical("载入数据源配置出错: 无法识别的字段类型: %s", v)
                raise Exception("载入数据源配置出错: 无法识别的字段类型: {}".format(v))
        table = type(str(table_name), (Table,), {'table_name': table_name, 'fields': fields})  # 动态类型
        """
           table_name(Table):
             table_name
             fields
            动态创建类 该类继承 Table
        """
        temp_map[table_name] = table
    for db_setting_config in config.get("db_settings", []):
        name = db_setting_config.get("name")
        tables = []
        for table_name in db_setting_config.get("tables", []):
            table = temp_map.get(table_name, None)
            if table is not None:
                temp_map[table_name] = "{}.{}".format(name, table_name)  # 其实这两个指向的都是同一个对象
                tables.append(table)
        db_settings.add(
            name=name,
            db_type=get_db_type(db_setting_config.get("db_type", None)),
            host=db_setting_config.get("host", None),
            port=db_setting_config.get("port", None),
            user=db_setting_config.get("user", None),
            password=db_setting_config.get("password", None),
            schema=db_setting_config.get("schema", None),
            db_name=db_setting_config.get("db_name", None),
            tables=tables,
            black_tables=db_setting_config.get("black_tables", None),
            black_fields=db_setting_config.get("black_fields", None),
        )

