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
    return ResponseModel.success(df.to_json(orient="columns", force_ascii=False))  # 默认是json，其他的形式不转化


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
