# encoding=utf-8
from restsql.restClient import restClient
from restsql.config.settings import *
if __name__ == '__main__':
    query_dict = {
            "from": "es_test.cars",
            "time": {
                "column": 'sold',
                "begin": "",
                "end": "",
                "interval": "1M"
            },
            "select": [
                {
                    "column": "price",
                    "alias": "price_sum",
                    "metric": "sum"
                }
            ],
            "where": [
                {
                    "column": "color",
                    "op": "=",
                    "value": "red"
                }
            ],
            "group": []
        }
    db_configs.put(
        DataBase(
            name='es_test',
            db_type=EnumDataBase.ES,
            host='127.0.0.1',
            port=9200
        )
    )
    client=restClient(query_dict)
    print(client.query())