import logging

from django.http import HttpResponse, HttpResponseBadRequest
from django.views.decorators.http import require_GET
import json
import sys
sys.path.extend([r'E:\f1ed-restsql-librestsql-master'])
from restsql import rest_client


from .config.exception import JsonFormatException, RunningException, UserConfigException, RestSqlExceptionBase, \
    ProgramConfigException
from .utils import ResponseModel
from .utils import frame_parse_obj
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
        client = rest_client.Client()
        client.query(rest_query)
    except RestSqlExceptionBase as e:
        return HttpResponse(ResponseModel.failure(e.code, "The query failed，the error message: {}".format(e.message)))
    resp = frame_parse_obj(client.result, return_format)  # 如果是不指明以txt,或者html格式的，在内部进行转json返回
    return HttpResponse(resp)


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
            resp = ResponseModel.success(table_dict)  # 内置转json
        except JsonFormatException as e:
            logging.exception(e)
            return HttpResponseBadRequest(ResponseModel.failure(e.code, "parse table_dict to json error"))
        return HttpResponse(resp)
    else:
        return HttpResponseBadRequest(ResponseModel.failure("400", "Incorrect request method"))


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


def helloword(request):
    return HttpResponse('ok')
