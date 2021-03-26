# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 17:19
# @Author  : Fcvane
# @Param   : 
# @File    : dialects_mysql.py
import mdtool
import sys


class MysqlDialect():
    def __init__(self, dbsrc, dbmgr, tables_in=None):
        self.tables_in = tables_in
        # values
        for value in dbsrc.values():
            self.host = value['host']
            self.port = value['port']
            self.user = value['user']
            self.passwd = value['passwd']
            self.dbname = value['dbname']
            self.dbtype = value['dbtype'].lower()
        # 调用工具类
        self.dbsrc_executor = mdtool.DbManager(self.host, self.port, self.user, self.passwd, self.dbname, self.dbtype)
        # values
        for value in dbmgr.values():
            self.host_mgr = value['host']
            self.port_mgr = value['port']
            self.user_mgr = value['user']
            self.passwd_mgr = value['passwd']
            self.dbname_mgr = value['dbname']
            self.dbtype_mgr = value['dbtype']
        # 管理库
        self.dbmgr_executor = mdtool.DbManager(self.host_mgr, self.port_mgr, self.user_mgr, self.passwd_mgr,
                                               self.dbname_mgr, self.dbtype_mgr)

    # 表信息
    def mdsyncer_tables(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from mdsyncer_tables where db_type='%s'" % self.dbtype, None)
            params = (self.dbtype, self.dbname)
            query = """
               SELECT 
                   %s,
                   table_schema,
                   table_name,
                   table_collation,
                   table_comment
               FROM information_schema.tables
               WHERE table_schema=%s
               """
            dataset = self.dbsrc_executor.dbfetchall(query, params)
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute("delete from mdsyncer_tables where db_type='%s' and table_name='%s'" % (
                    self.dbtype, tab), None)
                params = (self.dbtype, self.dbname, tab)
                query = """
                SELECT 
                    %s,
                    table_schema,
                    table_name,
                    table_collation,
                    table_comment
                FROM information_schema.tables
                WHERE table_schema=%s
                AND table_name=%s
                """
                result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in result:
                    dataset.append(elem)
                # 表不存在写入日志
                if len(result) == 0:
                    mdtool.log.warning("%s表不存在于%s数据库,请检查" % (tab, self.dbtype))
        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO mdsyncer_tables
            (db_type,
            table_schema,
            table_name,
            table_collation,
            table_comment) 
        VALUES
            (%s, %s, %s, %s, %s)
        """
        # 加载数据前先删除历史记录
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s表信息数据加载到mdsyncer库表mdsyncer_tables成功" % self.dbtype)

    # 字段信息
    def mdsyncer_columns(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from mdsyncer_columns where db_type='%s'" % self.dbtype, None)
            params = (self.dbtype, self.dbname)
            query = """
                SELECT 
                    %s,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position,
                    CASE WHEN 
                        (column_default IS NOT NULL AND data_type = 'char') 
                        or 
                        (column_default IS NOT NULL AND data_type = 'varchar' )
                        then concat("'",column_default,"'")
                    ELSE column_default END column_default,
                    CASE WHEN
                        is_nullable = 'YES' THEN 'Y'
                    ELSE
                        'N' END is_nullable,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    character_set_name,
                    collation_name,
                    column_type,
                    column_comment
                FROM information_schema.columns
                WHERE table_schema = %s
                   """
            dataset = self.dbsrc_executor.dbfetchall(query, params)
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from mdsyncer_columns where db_type='%s' and table_name='%s'" % (self.dbtype, tab), None)
                params = (self.dbtype, self.dbname, tab)
                query = """
                SELECT
                    %s,
                    table_schema,
                    table_name,
                    column_name,
                    ordinal_position,
                    CASE WHEN 
                        (column_default IS NOT NULL AND data_type = 'char') 
                        or 
                        (column_default IS NOT NULL AND data_type = 'varchar' )
                        then concat("'",column_default,"'")
                    ELSE column_default END column_default,
                    CASE WHEN
                        is_nullable = 'YES' THEN 'Y'
                    ELSE
                        'N' END is_nullable,
                    data_type,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale,
                    character_set_name,
                    collation_name,
                    column_type,
                    column_comment
                FROM information_schema.columns
                WHERE table_schema = %s
                AND table_name = %s
                """
                result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in result:
                    dataset.append(elem)
        # 写入dbsyncer管理库 - mysql
        sql = """
            INSERT INTO mdsyncer_columns
                (db_type,
                table_schema,
                table_name,
                column_name,
                ordinal_position,
                column_default,
                is_nullable,
                data_type,
                character_length,
                numeric_precision,
                numeric_scale,
                character_set_name,
                collation_name,
                column_type,
                column_comment) 
            VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s字段信息数据加载到mdsyncer库表mdsyncer_columns成功" % self.dbtype)

    # 约束信息
    # 从MySQL 5.7开始 ：该CHECK子句已解析，但被所有存储引擎忽略
    def tables_constraints(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from tables_constraints where db_type='%s'" % self.dbtype, None)
            params = (self.dbtype, self.dbname)
            query = """
                SELECT 
                    %s,
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    tc.constraint_type,
                    kcu.column_name,
                    kcu.ordinal_position,
                    kcu.referenced_table_name,
                    kcu.referenced_column_name,
                    kcu.ordinal_position
                FROM information_schema.table_constraints tc,
                    information_schema.key_column_usage kcu
                WHERE tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                    AND tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = %s
                   """
            dataset = self.dbsrc_executor.dbfetchall(query, params)
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from tables_constraints where db_type='%s' and table_name='%s'" % (self.dbtype, tab), None)
                params = (self.dbtype, self.dbname, tab)
                query = """
                SELECT 
                    %s,
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    tc.constraint_type,
                    kcu.column_name,
                    kcu.ordinal_position,
                    kcu.referenced_table_name,
                    kcu.referenced_column_name,
                    kcu.ordinal_position
                FROM information_schema.table_constraints tc,
                    information_schema.key_column_usage kcu
                WHERE tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                    AND tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                """
                result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in result:
                    dataset.append(elem)
        # 写入dbsyncer管理库 - mysql
        sql = """
            INSERT INTO tables_constraints
                (db_type,
                table_schema,
                table_name,
                constraint_name,
                constraint_type,
                column_name,
                ordinal_position,
                referenced_table_name,
                referenced_column_name,
                referenced_ordinal_position) 
            VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s约束信息数据加载到mdsyncer库表tables_constraints成功" % self.dbtype)

    # 索引信息
    def tables_indexes(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from tables_indexes where db_type='%s'" % self.dbtype, None)
            params = (self.dbname,)
            query = """
                SELECT 
                    CONCAT (
                    table_schema,
                    '.',
                    table_name
                    ) table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                   """
            tables_result = self.dbsrc_executor.dbfetchall(query, params)
            dataset = []
            for elem in tables_result:
                table_name = elem[0]
                query = """
                SHOW INDEX FROM %s
                """ % table_name
                result = self.dbsrc_executor.dbfetchall(query, None)
                if result is None:
                    pass
                else:
                    for i in result:
                        # 剔除主键
                        if 'PRIMARY' in i[2]:
                            pass
                        else:
                            if i[1] == 0:
                                dataset.append((self.dbtype, self.dbname, i[0], 'UNIQUE', i[2], i[4], i[3], i[10]))
                            elif i[1] == 1:
                                dataset.append((self.dbtype, self.dbname, i[0], 'NONUNIQUE', i[2], i[4], i[3], i[10]))
                            else:
                                mdtool.log.error("索引数据异常")
                                sys.exit()
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from tables_indexes where db_type='%s' and table_name='%s'" % (self.dbtype, tab), None)
                params = (self.dbname, tab)
                query = """
                SELECT 
                    CONCAT (
                    table_schema,
                    '.',
                    table_name
                    ) table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                and table_name = %s
                """
                tables_result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in tables_result:
                    table_name = elem[0]
                    query = """
                    SHOW INDEX FROM %s
                    """ % table_name
                    result = self.dbsrc_executor.dbfetchall(query, None)
                    if result is None:
                        pass
                    else:
                        for i in result:
                            # 剔除主键
                            if 'PRIMARY' in i[2]:
                                pass
                            else:
                                if i[1] == 0:
                                    dataset.append((self.dbtype, self.dbname, i[0], 'UNIQUE', i[2], i[4], i[3], i[10]))
                                elif i[1] == 1:
                                    dataset.append(
                                        (self.dbtype, self.dbname, i[0], 'NONUNIQUE', i[2], i[4], i[3], i[10]))
                                else:
                                    mdtool.log.error("索引数据异常")
                                    sys.exit()
        # 写入dbsyncer管理库 - mysql
        sql = """
            INSERT INTO tables_indexes
                (db_type,
                table_schema,
                table_name,
                uniqueness,
                index_name,
                column_name,
                ordinal_position,
                index_type) 
            VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s)
               """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s索引数据数据加载到mdsyncer库表tables_indexes成功" % self.dbtype)


if __name__ == '__main__':
    dbsrc = mdtool.xmler('MYSQL_172.21.86.205').dbCFGInfo()
    dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    # tables_in = 'dept,dsr_work_order_history,emp'
    # dialect = MysqlDialect(dbsrc, dbmgr, tables_in)
    dialect = MysqlDialect(dbsrc, dbmgr)
    dialect.mdsyncer_tables()
    dialect.mdsyncer_columns()
    dialect.tables_constraints()
    dialect.tables_indexes()
