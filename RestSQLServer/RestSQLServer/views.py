import logging

from django.http import HttpResponse
from django.views.decorators.http import require_GET
import json
from restsql import restclient
from .util import ExceptionEnum
from .util import ResponseModel
from .util import frame_parse_obj


def testSourceView(request):
    return HttpResponse('pk')


def searchSourceView(request):
    return HttpResponse('ok')


def querySourceView(request):
    restquery = json.loads(request.body)
    print(restquery)

    from_item = restquery['from']
    # 这里可以加错误处理

    client = restclient.Client()

    # 返回结果
    flag = client.query(restquery)
    # if flag:
    #     result = client.result
    #     return HttpResponse('ok')

    return HttpResponse('false')


@require_GET
def apiquery(request):
    if request.body is None:
        return HttpResponse(ResponseModel.failure_response('error'))
    restquery = json.loads(request.body)
    return_format = request.GET.get('format', None)
    logging.DEBUG(restquery)
    if not restquery.get('from', None):
        return HttpResponse(ResponseModel.failure_response('error', ExceptionEnum))
    client = restclient.Client()
    result = client.query(restquery)
    if not result:
        return HttpResponse(ResponseModel.failure_response('error', 'unknown error'))
    resp = frame_parse_obj(result, return_format)
    return HttpResponse(json.dumps(resp))


def helloword(request):
    return HttpResponse('ok')
