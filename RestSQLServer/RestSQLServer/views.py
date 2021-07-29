from django.http import HttpResponse
import json
import re
from restsql.datasource.es import es_entry
from restsql.datasource.sql import sql_entry
from restsql.config import utils
from restsql.datasource import query_client


def testSourceView(request):
    # 假设传输from参数示例
    # {
    #     from: 'test.ccc'
    # }
    print(request.body)
    body = json.loads(request.body)

    print(body)
    if body and body['from']:  # 查找得到body里面from的参数
        source = body['from']
        p = re.compile(r'\W+')
        from_sub = p.split(body)

        datasource = utils.get_datasource_bysource(from_sub[0])
        if datasource:  # 查找 里面包含的db 是否已经在文件里面
            return HttpResponse('ok')
    return HttpResponse('false')


def searchSourceView(request):
    return HttpResponse('ok')


def querySourceView(request):
    pid = request.COOKIES.get('pid')
    restquery = json.loads(request.body)
    print(restquery)

    from_item = restquery['from']
    # 这里可以加错误处理

    client = query_client.Client(from_item, pid)

    # 返回结果
    flag = client.query(restquery)
    if flag:
        result = client.result
        return HttpResponse('ok')

    return HttpResponse('false')


def helloword(request):
    return HttpResponse('ok')
