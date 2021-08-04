import datetime

__all__ = ['ResponseModel', 'frame_parse_obj']

# 转dataframe为html格式
import json


def frame_parse_obj(df, key='json'):
    """
    :param df:传入的返回dateframe
    :param key: 指定返回的格式
    :return: 返回转化后的格式
    """
    if key == 'html':
        return df.to_html()
    elif key == 'latex':
        return df.to_latex()
    elif key == 'txt':
        return df.to_string()
    return ResponseModel.success(df.to_dict('records'))
    """
     1.默认最终返回是json， ,但是该模板类Response返回时会进行json.dump,所以这里先把dataframe转dict,然后再进行统一转json
     防止同一个对象连续两次json.dump出现在前端显示时，相关字段被强行转义加'\'
     2. 该模式下，使用records模式反序列化，如需要更换返回的表结构模式，具体查阅pandas to_dict文档
    """


class ResponseModel:

    def __init__(self, status):
        self._result = {
            'code': status,
            'time': datetime.datetime.now().strftime("  %Y-%m-%d %I:%M:%S %p ")
        }

    @staticmethod
    def success(data):
        resp = ResponseModel('200')
        resp._result['data'] = data
        return json.dumps(resp._result)

    @staticmethod
    def failure(status, msg):
        resp = ResponseModel(status)
        resp._result['msg'] = msg
        return json.dumps(resp._result)
