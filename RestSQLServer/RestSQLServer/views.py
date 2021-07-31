from django.http import HttpResponse
import json

from restsql import restclient


def testSourceView(request):

    return HttpResponse('false')


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


def helloword(request):
    return HttpResponse('ok')
