from restsql.datasource.pg_orm_client import Query, loads1, db_settings, PgClient  # 测试环境，在datasource文件夹中模拟读取配置文件过程

loads1.init_json()
database = db_settings.get_by_name("source2")


# 可以复制下面这段测试协议，启动django ,访问api进行测试
querysql = {
    "from": "source2.model1_testuser",
    "time": {
        "column": "datetime",
        "begin": "2021-08-01 19:24:29.000000",
        "end": "2021-08-02 9:24:29.000000",
        "interval": "10S"
    },
    "select": [
        {
            "column": "username",
            "alias": "myusername",
            "metric": "count"
        }
    ],
    "where": [
        {
            "column": "username",
            "op": "!=",
            "value": "ccc"
        },
        {
            "column": "password",
            "op": "startswith",
            "value": "f"
        }
    ],
    "group": []
}
pg_client = PgClient(database)
que = Query(querysql)
result = pg_client.query(que)
print(result)
