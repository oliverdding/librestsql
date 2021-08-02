import logging

from django.http import HttpResponse
from django.views.decorators.http import require_GET
import json
from restsql import rest_client
from .util import ExceptionEnum
from .util import ResponseModel
from .util import frame_parse_obj
from restsql.config.database import db_settings


def testSourceView(request):
    return HttpResponse('pk')


def searchSourceView(request):
    return HttpResponse('ok')


def querySourceView(request):
    restquery = json.loads(request.body)
    print(restquery)

    from_item = restquery['from']
    # 这里可以加错误处理

    client = rest_client.Client()

    # 返回结果
    flag = client.query(restquery)
    # if flag:
    #     result = client.result
    #     return HttpResponse('ok')

    return HttpResponse('false')


@require_GET
def apiquery(request):
    if request.body is None:
        return HttpResponse(ResponseModel.failure('error'))
    rest_query = json.loads(request.body)
    return_format = request.GET.get('format', None)
    if not rest_query.get('from', None):
        return HttpResponse(ResponseModel.failure('error', ExceptionEnum))
    client = rest_client.Client()
    flag = client.query(rest_query)
    if flag is False:
        return HttpResponse(ResponseModel.failure_response('error', 'unknown error'))
    resp = ResponseModel.success(frame_parse_obj(client.result, return_format))
    return HttpResponse(json.dumps(resp))


def table_query(request):
    """
    :param request: 获取需要查询表的database
    :return: 返回json格式的表结构列表
    """
    if request.method == "GET":
        database_name = request.GET.get("database")
        database = db_settings.get_by_dbname(database_name)
        table_dict = {}
        for t in database.tables:
            table_dict[getattr(t, 'table_name')] = list(
                getattr(t, 'fields').keys())  # {'table_name':['column1','column2']
        try:
            resp = json.dumps(ResponseModel.success(table_dict))
        except Exception as e:
            logging.exception(e)
            raise Exception("parse table_dict to json error")
        return HttpResponse(resp)
    else:
        return HttpResponse(json.dumps(ResponseModel.failure('bad', ExceptionEnum.RequestMethodError)))


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
        except Exception as e:
            logging.exception(e)
            raise Exception("parse database_list to json error")
        return HttpResponse(resp)
    else:
        return HttpResponse(json.dumps(ResponseModel.failure('bad', ExceptionEnum.RequestMethodError)))


def helloword(request):
    return HttpResponse('ok')
