from django.http import HttpResponse
import json
import re
from restsql.config import utils

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

        datasource = utils.get_datasource_byfromsub(from_sub[0])
        if datasource:  # 查找 里面包含的db 是否已经在文件里面
            return HttpResponse('ok')
    return HttpResponse('false')


def searchSourceView(request):
    return HttpResponse('ok')


def querySourceView(request):
    pid = request.COOKIES.get('pid')
    restsql = json.loads(request.body)
    print(restsql)

    from_item = restsql['from']
    p = re.compile(r'\W+')  # test.table ，分离出单词
    from_sub = p.split(from_item)
    # 之后加入错误处理，考虑不符合规则
    print(list)

    db = utils.get_datasource_byfromsub(from_sub[0])
    if not db:
        return HttpResponse("false")

    typeparam = db['type']

    if typeparam == 'es':
        client = es_entry.EsClient(db)
    elif typeparam == 'sql':
        client = sql_entry.SQLClient(db)
    else:
        return HttpResponse("false")
    table = utils.get_table_bydbname(from_sub[0])
    real_tablename = utils.get_realname_bytable(table, from_sub[1])
    # 返回结果
    result = client.sql_query(real_tablename, restsql, {},pid)
    print(result)
    return HttpResponse(result.to_json())

def helloword(request):
    return HttpResponse('ok')
