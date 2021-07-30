from restsql.config import db_setting


def get_datasource_bysource(dbname):
    db = db_setting.db_configs.get_db_bydbname(dbname)
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


def get_blacktables_bysource(dbname):
    db = db_setting.db_configs.get_db_bydbname(dbname)
    return db.get('blacktables', [])


def get_blackcolumn_bytable(dbname, tablename):
    column = db_setting.db_configs.get_blackcolumn_bytablename(dbname, tablename)
    return column


def get_struct_bytable(dbname, tablename):
    struct = db_setting.db_configs.get_tablestruct_byname(dbname, tablename)
    return struct
