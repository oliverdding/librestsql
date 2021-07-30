# -*- coding:utf-8 -*-

class Table(object):
    """
    子类需要包含table_name=”  “
              fields={
                ”字段名：”字段类型“,,,
              }
    """
    pass


class Field(object):
    pass


"""
如果要采用orm需要使用的类型映射类
"""


class IntField(Field):
    """
    整数类型: IntegerField
    """
    pass


class NumberField(Field):
    """
    数字类型: DoubleField
    """
    pass


class StringField(Field):
    """
    字符串类型: TextField
    """
    pass


class BoolField(Field):
    """
    布尔类型: BooleanField
    """
    pass
