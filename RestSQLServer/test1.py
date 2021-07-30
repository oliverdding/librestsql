from restsql.config import file_setting, db_setting
#
#
file_setting.init_db_config()
print(db_setting.db_configs)
print('ok')
print(db_setting.db_configs.config['tables']['source2'])
print(db_setting.db_configs.config['db_settings'].keys())
print(db_setting.db_configs.get_db_list())
print(db_setting.db_configs.get_db_bydbname('source1'))
print("打印总结构")
print(db_setting.db_configs.config)
print("打印黑名单字段")
print(db_setting.db_configs.config['tables_struct'])
print("打印黑名单字段")
print(db_setting.db_configs.config['columns_black'])

# import psycopg2
# conn=None
#
# # conn = psycopg2.connect(database='testdjango', user='grafana', password='zhang23785491', host='127.0.0.1', port=5432)
# conn= psycopg2.connect(database="testdjango", user="postgres", password="zhang23785491", host="127.0.0.1", port="5432")
# if not conn:
#     print("false")
# else:
#     cur = conn.cursor()
#     table = 'Model1_testuser'
#     restsql = {'from': 'xxx',
#                'select': [{'column': 'username', 'alias': 'name'}, {'column': 'password', 'alias': 'password'}],
#                'where': [], 'group': [], 'limit': 5}
#     sql = 'select {} from {} '
#     select_sql = ""
#     select = restsql.get('select', [])
#     for item in select:
#         select_sql += item.get('column', "")
#         if item.get('alias', None):
#             select_sql += " as " + item.get('alias')
#         select_sql += ','
#
#     print(select_sql)
#
#     sql = sql.format(select_sql[0:-1], 'public."Model1_testuser"')
#
#     print(sql)
#     cur.execute(sql)
#     rows=cur.fetchall()
#     for row in rows:
#         print(row)
#
#     conn.commit()
#     conn.close()