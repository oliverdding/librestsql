from restsql.config import setting,db_setting


setting.init_db_config()
print(db_setting.db_configs)
print('ok')
print(db_setting.db_configs.config['tables']['source2'])
print(db_setting.db_configs.config['db_settings'].keys())
print(db_setting.db_configs.get_db_list())
print(db_setting.db_configs.get_by_dbname('source1'))
print(db_setting.db_configs)