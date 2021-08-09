# encoding=utf-8
import pandas as pd
from restsql.rest_client import *
from restsql.config.database import *

if __name__ == '__main__':
    query_dict = {
        "from": "es_test.cars",
        "time": {
            "column": 'sold',
            "begin": "2001-08-08",
            "end": "2019-08-08 01:01:01",
            "interval": "1s"
        },
        "select": [
            {
                "column": "price",
                "alias": "价格",
                "metric": "count"
            },
            {
                "column": "sold",
                "alias": "销售时间",
                "metric": ""
            },
        ],
        "where": [

        ],
        "group": [],
        "limit": 1000
    }
    db_settings.add(
        name='es_test',
        db_type=EnumDataBase.ES,
        host='127.0.0.1',
        port=9200
    )
    client = RestClient(query_dict)
    result = client.query()
    print(result["time"].values.tolist())
