# encoding=utf-8
import pandas as pd
from restsql.rest_client import *
from restsql.config.database import *

if __name__ == '__main__':
    query_dict = {
        "from": "es_test.test_index",
        "time": {
            "column": "time",
            "begin": "",
            "end": "",
            "interval": "1y"
        },
        "select": [
            {
                "column": "price",
                "alias": "",
                "metric": "sum"
            },
            {
                "column": "price",
                "alias": "",
                "metric": "max"
            },
            {
                "column": "address.keyword",
                "alias": "",
                "metric": "count"
            }
        ],
        "group": ["cityName.keyword"]
    }

    db_settings.add(
        name='es_test',
        db_type=EnumDataBase.ES,
        host='127.0.0.1',
        port=9200
    )
    client = RestClient(query_dict)
    result = client.query()
    print(result)
