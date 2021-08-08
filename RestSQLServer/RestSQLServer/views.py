import logging

from django.http import HttpResponse, HttpResponseBadRequest
from django.views.decorators.http import require_POST
import json
import sys

sys.path.extend([r'E:\f1ed-restsql-librestsql-master'])
from restsql import rest_client

from .config.exception import JsonFormatException, RunningException, UserConfigException, RestSqlExceptionBase, \
    ProgramConfigException
from .utils import ResponseModel
from .utils import frame_parse_obj
from restsql.config.database import db_settings


def test(request):
    return HttpResponse('ok')


def grafana_search(request):
    return HttpResponse('ok')


def grafana_query(request):
    # print("grafana请求测试")
    # print(json.loads(request.body))
    # TODO 下一步进行grafana对接
    # 测试返回的假数据
    test_response = [
        {
            "target": "upper_75",
            "datapoints": [
                [622, 1450754160000],
                [365, 1450754220000],
                [465, 1450764220000],
                [665, 1450774220000],
                [865, 1450784220000],
                [265, 1450794220000],
                [665, 1450804220000],
                [865, 1450814220000],
                [965, 1450824220000],

            ]
        },
        {
            "target": "upper_90",
            "datapoints": [
                [861, 1450754160000],
                [767, 1450754220000]
            ]
        }
    ]
    result = {'status': 'ok',
              'data': test_response}
    print("grafana 测试返回结果")
    print(result)
    return HttpResponse(json.dumps(result))


@require_POST
def apiquery(request):
    """
    :param request: 获取请求需要的query,以及而外的参数，如format,额外希望返回的格式
    :return 默认返回json格式结构数据，除非使用format参数指令其他的返回类型
    """
    if request.body is None:
        return HttpResponseBadRequest(ResponseModel.failure('error', "Please input the request query"))

    try:
        rest_query = json.loads(request.body)
    except JsonFormatException as e:
        logging.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure(e.code, e.message))
    return_format = request.GET.get('format', None)
    if not rest_query.get('from', None):
        return HttpResponse(ResponseModel.failure('400', "You need to specify the target data source"))
    try:
        client = rest_client.RestClient(rest_query)
        result = client.query()
    except RestSqlExceptionBase as e:
        return HttpResponse(ResponseModel.failure(e.code, "The query failed，the error message: {}".format(e.message)))
    resp = frame_parse_obj(result, return_format)  # 如果是不指明以txt,或者html格式的，在内部进行转json返回
    return HttpResponse(resp)


def table_query(request):
    """
    :param request: 获取需要查询表的database
    :return: 返回json格式的表结构列表
    """
    # if request.method == "GET":
    #     database_name = request.GET.get("database")
    #     database = db_settings.get_by_dbname(database_name)
    #     table_dict = {}
    #     for t in database.tables:
    #         table_dict[getattr(t, 'table_name')] = list(
    #             getattr(t, 'fields').keys())  # {'table_name':['column1','column2']
    #     try:
    #         resp = ResponseModel.success(table_dict)  # 内置转json
    #     except JsonFormatException as e:
    #         logging.exception(e)
    #         return HttpResponseBadRequest(ResponseModel.failure(e.code, "parse table_dict to json error"))
    #     return HttpResponse(resp)
    # else:
    #     return HttpResponseBadRequest(ResponseModel.failure("400", "Incorrect request method"))
    return HttpResponse(json.dumps({
        'status': 'ok',
        'data': ["test1", "test2"]
    }))


def database_query(request):
    """
    :return:返回json格式的数据源名列表
    """
    if request.method == "GET":
        databases = []
        for k in db_settings.get_all_name():
            databases.append(k)
        try:
            resp = json.dumps(ResponseModel.success(databases))
        except JsonFormatException as e:
            logging.exception(e)
            return HttpResponseBadRequest(ResponseModel.failure(e.code, "parse table_dict to json error"))
        return HttpResponse(resp)
    else:
        return HttpResponseBadRequest(ResponseModel.failure("400", "Incorrect request method"))
