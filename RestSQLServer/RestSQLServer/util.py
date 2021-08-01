import datetime

__all__ = ['ResponseModel', 'ExceptionEnum', 'frame_parse_obj']


# 转dataframe为html格式
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
    return ResponseModel.success_response(df.to_json(orient="columns", force_ascii=False))


class ExceptionEnum:
    NullPoint = {"code": "301", "exception": "Null point", "typedesc": "empty point"}
    NoSource = {"code": "302", "exception": "No object specified", "typedesc": "the 'from' field is empty"}


class ResponseModel:

    def __init__(self, status, data):
        self._result = {
            'status': status,
            'data': data,
            'time': datetime.datetime.now().strftime("  %Y-%m-%d %I:%M:%S %p ")
        }

    @staticmethod
    def success_response(data):
        resp = ResponseModel('ok', data)
        return resp._result

    @staticmethod
    def failure_response(status, data):
        ResponseModel(status, data)
