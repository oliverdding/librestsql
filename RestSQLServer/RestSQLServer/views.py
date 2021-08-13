import json
import sys
import pandas as pd
from django.http import HttpResponse, HttpResponseBadRequest
from django.views.decorators.http import require_POST
from restsql import rest_client
from restsql.config.database import db_settings
from restsql.config.logger import rest_logger
from .config.exception import *
from .config.load import table_map
from RestSQLServer.RestSQLServer.query_manager import QueryManager
from .utils import ResponseModel
from .utils import frame_parse_obj, gen_restsql_query

sys.path.extend([r'E:\f1ed-restsql-librestsql-master'])


def test(request):
    return HttpResponse('ok')


def grafana_search(request):
    """
       查询variable接口
       request:
       {
           'target': '...' # example: {"select": "color", "from": "cars"}
       }
       response:
           ['...', ...]
       """
    rest_logger.logger.info("/search request")
    data = json.loads(request.body)
    target = data.get('target')
    if target is None:
        rest_logger.logger.error("target is invalid")
        raise UserConfigException('target is invalid')
    try:
        target = json.loads(target)
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    try:
        restsql_query = gen_restsql_query(target)
        rest_logger.logger.debug("VariableView: %s", restsql_query)
        client = rest_client.RestClient(restsql_query)
        results = client.query()
        rest_logger.logger.debug("VariableView: %s", results)
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponse(ResponseModel.failure("error", "The query failed，the error message: {}".format(e.args[0])))
    results = results[target["select"]].values.tolist()
    print(results)
    try:
        resp = json.dumps({'status': 'ok',
                           'data': results})
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    return HttpResponse(resp)


def grafana_query(request):
    """
    前端采用异步方式请求,只需要返回单次查询结果而无需以数组方式返回多次查询的结果。
    :param request: 请求对象
    :return: grafana指定的DataFrame格式
    """

    if request.body is None or request.body == "":
        rest_logger.logger.warning("request body is Empty")
        return HttpResponseBadRequest(ResponseModel.failure("error", "Please input the request query"))
    # 权限控制检查
    queryManager = QueryManager()
    if not queryManager.query_check():
        return HttpResponseBadRequest(ResponseModel.failure("error", "Access Denied"))
    try:
        rest_query = json.loads(request.body)
        rest_logger.logger.debug("query: {}".format(rest_query))
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    # 通过协议的表名在请求协议中添加数据源
    table_name = rest_query["from"].split(".")[1]
    rest_query["from"] = table_map.get(table_name, "")
    if rest_query["from"] == "":
        rest_logger.logger.error("Table {} is not found".format(table_name))
        return HttpResponseBadRequest(ResponseModel.failure("error", "Table not found"))
    try:
        client = rest_client.RestClient(rest_query)
        result = client.query()
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponse(ResponseModel.failure("error", "The query failed，the error message: {}".format(e.args[0])))
    refId = rest_query.get("refId", "A")
    # pandas dataFrame转化为grafana的dataframe格式
    resp = {"refId": refId, "fields": []}
    timeShift = rest_query["time"].get("timeShift", 0)
    for column in result.columns:
        if column == "time":
            result["time"] = pd.to_datetime(result["time"])
            fieldDTO = {"name": column, "values": [int(t) // 10 ** 6 - timeShift for t in result[column].values], "type": "time"}
        else:
            fieldDTO = {"name": column, "values": result[column].values.tolist()}
        resp["fields"].append(fieldDTO)
    try:
        result = json.dumps({'status': 'ok',
                             'data': resp}
                            )
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    return HttpResponse(result)


def grafana_tables(request):
    """
      查询可选表的接口
      response:
      {
          'status': 'ok',
          'data': ['...', ...]
      }
      Args:
          request: 传输的http对象
      Returns:
          status: ok/error
          data: 返回表名列表
      """
    try:
        resp = json.dumps({
            'status': 'ok',
            'data': list(table_map.keys())
        })
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.message))
    return HttpResponse(resp)


def grafana_options(request):
    """
      查询表字段接口
      request:
          {
              'tableName': '...'<String>
          }
      response:
          ['...', ...]
    """
    if request.body is None or request.body == "":
        rest_logger.logger.warning("request body is empty")
        return HttpResponseBadRequest(ResponseModel.failure('error', "Please input the request query"))
    try:
        request_dict = json.loads(request.body)
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    table_name = request_dict.get("tableName", "")
    db_table_name = table_map.get(table_name, None)
    if db_table_name is None:
        rest_logger.logger.warning('Could not find table entry: %s', table_name)
        return HttpResponseBadRequest(
            ResponseModel.failure("error", 'Could not find table entry: {}'.format(table_name)))
    try:
        db_name, table_name = db_table_name.split('.', 1)
    except BaseException:
        rest_logger.logger.warning('table name %s is invalid', db_table_name)
        raise Exception('table name {} is invalid'.format(db_table_name))
    target_table = None
    for db_setting_name in db_settings.get_all_name():
        if db_name == db_setting_name:
            for table in db_settings.get_by_name(db_setting_name).tables:
                if table.table_name == table_name:
                    target_table = table
                    break
            break  # target db has no target table
    if target_table is None:
        rest_logger.logger.warning('Could not find table: %s', db_table_name)
        return HttpResponseBadRequest(ResponseModel.failure("error", 'Could not find table: {}'.format(db_table_name)))
    try:
        resp = json.dumps({
            'status': 'ok',
            'data': list(target_table.fields.keys())
        })
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    return HttpResponse(resp)


@require_POST
def api_query(request):
    """
    :param request: 获取请求需要的query,以及而外的参数，如format,额外希望返回的格式
    :return 默认返回json格式结构数据，除非使用format参数指令其他的返回类型
    """
    if request.body is None:
        return HttpResponseBadRequest(ResponseModel.failure('error', "Please input the request query"))
    # 权限检查
    queryManager = QueryManager()
    if not queryManager.query_check():
        return HttpResponseBadRequest(ResponseModel.failure("error", "Access Denied"))
    try:
        rest_query = json.loads(request.body)
        rest_logger.logger.debug("restapi query： {}".format(rest_query))
    except Exception as e:
        rest_logger.logger.exception(e)
        return HttpResponseBadRequest(ResponseModel.failure("error", e.args[0]))
    return_format = request.GET.get('format', None)
    if not rest_query.get('from', None):
        return HttpResponse(ResponseModel.failure('400', "You need to specify the target data source"))
    try:
        client = rest_client.RestClient(rest_query)
        result = client.query()
    except RestSqlExceptionBase as e:
        return HttpResponse(ResponseModel.failure("error", "The query failed，the error message: {}".format(e.message)))
    resp = frame_parse_obj(result, return_format)  # 如果是不指明以txt,或者html格式的，在内部进行转json返回
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
        except Exception as e:
            rest_logger.logger.exception(e)
            return HttpResponseBadRequest(ResponseModel.failure(e.code, "parse table_dict to json error"))
        return HttpResponse(resp)
    else:
        return HttpResponseBadRequest(ResponseModel.failure("400", "Incorrect request method"))


def database_query(request):
    # """
    # :return:返回json格式的数据源名列表
    # """
    """函数功能.

            函数功能说明.

            Args:
                arg1 (int): arg1的参数说明
                arg2 (str): arg2的参数说明

            Returns:
                bool: 返回值说明

    """
    if request.method == "GET":
        databases = []
        for k in db_settings.get_all_name():
            databases.append(k)
        try:
            resp = json.dumps(ResponseModel.success(databases))
        except Exception as e:
            rest_logger.logger.exception(e)
            return HttpResponseBadRequest(ResponseModel.failure(e.code, "parse table_dict to json error"))
        return HttpResponse(resp)
    else:
        return HttpResponseBadRequest(ResponseModel.failure("400", "Incorrect request method"))
