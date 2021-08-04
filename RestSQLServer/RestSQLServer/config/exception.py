__all__ = ['RestSqlExceptionBase', 'RunningException', 'UserConfigException', 'JsonFormatException',
           'ProgramConfigException']


class RestSqlExceptionBase(Exception):
    """
    RestSQL库所有异常的基类。
    code 0
    """

    def __init__(self, code, message, *args):
        self.code = code
        self.message = message.format(args)


class RunningException(RestSqlExceptionBase):
    """
    运行时异常；查看日志并处理。
    code [1, 100]
    """
    pass


class UserConfigException(RestSqlExceptionBase):
    """
    用户相关异常；提示用户。
    code [101, 200]
    """
    pass


class JsonFormatException(UserConfigException):
    """
    json格式异常；提示用户问题。
    code [201, 300]
    """
    pass


class ProgramConfigException(RestSqlExceptionBase):
    """
    检查异常；检查程序。
    code [301, 400]
    """
    pass
