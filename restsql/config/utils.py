from restsql.config import db_setting


def get_datasource_byfromsub(dbname):
    db = db_setting.db_configs.get_by_dbname(dbname)
    return db


def get_realname_bytable(table, tablename):
    temptable = table.get(tablename, None)
    if temptable:
        return temptable
    else:
        return tablename


def get_table_bydbname(dbname):
    table = db_setting.db_configs.get_table_byname(dbname)
    return table
