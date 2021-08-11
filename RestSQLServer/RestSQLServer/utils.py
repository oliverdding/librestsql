import datetime

__all__ = ['ResponseModel', 'frame_parse_obj','gen_restsql_query']

# 转dataframe为html格式
import json

from .config.load import table_map


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


def gen_restsql_query(target):
    """
    根据variable查询的输入语句，生成restql查询
    :param target: 查询variable接口请求target中的内容
    :return:restsql请求协议
    """
    return {
        "from": table_map[target["from"]],
        "select": [
            {
                "column": target["select"],
                "alias": "",
                "metric": ""
            }
        ],
        "where": [],
        "group": [],
        "limit": 1000
    }


class ResponseModel:

    def __init__(self, status):
        self._result = {
            'status': status,
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
