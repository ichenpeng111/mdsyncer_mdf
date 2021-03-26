# -*- coding: utf-8 -*-

# @Author  : 陈朋
# @Time    : 2021/3/25 18:24
# @Param   : 
# @File    : cp_test.py
# @Software: mdsyncer

import mdtool
import sys


db_array='MYSQL_134.96.188.49_iom_inst1'
dbsrc = mdtool.xmler(db_array).dbCFGInfo()
for value in dbsrc.values():
    host = value['host']
    port = value['port']
    user = value['user']
    passwd = value['passwd']
    dbtype = value['dbtype'].lower()
    dbname = value['dbname'].lower()

dbsrc_executor = mdtool.DbManager(host, port, user, passwd, dbname, dbtype)

keys = 'MYSQL_134.96.188.49_iom_inst1'
params = 'iom_inst1'
query = """
        SELECT 
            CONCAT (
            table_schema,
            '.',
            table_name
            ) table_name
        FROM information_schema.tables
        WHERE table_schema = %s limit 50
           """
tables_result = dbsrc_executor.dbfetchall(query, params)
print(tables_result)
dataset = []
for elem in tables_result:
    table_name = elem[0]
    query = """
        SHOW INDEX FROM %s
        """ % table_name
    result = dbsrc_executor.dbfetchall(query, None)
    if result is None:
        pass
    else:
        print(1)
        print(result)
        print(table_name)
        for i in result:
            # 剔除主键
            if 'PRIMARY' in i[2]:
                pass
            else:
                if i[1] == 0:
                    dataset.append(
                        (keys, dbtype, dbname, i[0], 'UNIQUE', i[2], i[4], i[3], i[10]))
                elif i[1] == 1:
                    dataset.append(
                        (keys, dbtype, dbname, i[0], 'NONUNIQUE', i[2], i[4], i[3], i[10]))
                else:
                    mdtool.log.error("索引数据异常")
                    sys.exit()