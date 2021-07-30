from restsql.config import file_setting,settings


file_setting.init_db_config()
print(settings.db_configs)
print('ok')
print(settings.db_configs.config['tables']['source2'])
print(settings.db_configs.config['db_settings'].keys())
print(settings.db_configs.get_db_list())
print(settings.db_configs.get_by_dbname('source1'))
print(settings.db_configs)