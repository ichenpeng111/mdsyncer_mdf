# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 17:19
# @Author  : Fcvane
# @Param   : 
# @File    : dialects_mysql.py
import mdtool
import sys


class MysqlDialect():
    def __init__(self, dbsrc, dbmgr):
        # keys
        for key in dbsrc.keys():
            self.keys = key
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

        # keys
        for key in dbmgr.keys():
            self.keys_mgr = key
        # values
        for value in dbmgr.values():
            self.host_mgr = value['host']
            self.port_mgr = value['port']
            self.user_mgr = value['user']
            self.passwd_mgr = value['passwd']
            self.dbname_mgr = value['dbname']
            self.dbtype_mgr = value['dbtype'].lower()
            self.schema = None
            if self.dbtype_mgr == 'postgresql':
                if value.get('schema'):
                    self.schema = value['schema']
                else:
                    self.schema = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 管理库缺失schema" % self.keys_mgr)
        # 管理库
        self.dbmgr_executor = mdtool.DbManager(self.host_mgr, self.port_mgr, self.user_mgr, self.passwd_mgr,
                                               self.dbname_mgr, self.dbtype_mgr, self.schema)

    # 表信息
    def mdsyncer_tables(self):
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_tables_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = (self.keys, self.dbtype, self.dbname)
        query = """
        SELECT 
            %s,
            %s,
            table_schema,
            table_name,
            table_collation,
            table_comment
        FROM information_schema.tables
        WHERE table_schema=%s
        """
        dataset = self.dbsrc_executor.dbfetchall(query, params)

        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO mdsyncer_tables_tabdb
            (auth_id,
            db_type,
            table_schema,
            table_name,
            table_collation,
            table_comment) 
        VALUES
            (%s, %s, %s, %s, %s, %s)
        """
        # 加载数据前先删除历史记录
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s表信息数据加载到mdsyncer库表mdsyncer_tables_tabdb成功" % self.dbtype)

    # 字段信息
    def mdsyncer_columns(self):
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_columns_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = (self.keys, self.dbtype, self.dbname)
        query = """
        SELECT 
            %s,
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
        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO mdsyncer_columns_tabdb
            (auth_id,
            db_type,
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
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s字段信息数据加载到mdsyncer库表mdsyncer_columns_tabdb成功" % self.dbtype)

    # 约束信息
    # 从MySQL 5.7开始 ：该CHECK子句已解析，但被所有存储引擎忽略
    def tables_constraints(self):
        self.dbmgr_executor.dbexecute(
            "delete from tables_constraints_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys),
            None)
        params = (self.keys, self.dbtype, self.dbname)
        query = """
            SELECT 
                %s,
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
        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO tables_constraints_tabdb
            (auth_id,
            db_type,
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
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s约束信息数据加载到mdsyncer库表tables_constraints_tabdb成功" % self.dbtype)

    # 索引信息
    def tables_indexes(self):
        self.dbmgr_executor.dbexecute(
            "delete from tables_indexes_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = (self.dbname,)
        # 注释 原用查询information_schema.tables，之后在通过SHOW INDEX FROM 查询索引
        # query = """
        # SELECT
        #     CONCAT (
        #     table_schema,
        #     '.',
        #     table_name
        #     ) table_name
        # FROM information_schema.tables
        # WHERE table_schema = %s
        #    """
        # tables_result = self.dbsrc_executor.dbfetchall(query, params)
        query = """
        select b.table_name,
               b.non_unique,
               b.index_name,
               b.seq_in_index,
               b.column_name,
               b.collation,
               b.cardinality,
               b.sub_part,
               b.packed,
               b.nullable,
               b.index_type,
               b.comment,
               b.index_comment
        from information_schema.tables a,
             information_schema.statistics b
        where a.table_schema = b.table_schema
          and a.table_name = b.table_name
          and a.table_schema = %s
        """
        tables_result = self.dbsrc_executor.dbfetchall(query, params)
        dataset = []
        # for elem in tables_result:
            # table_name = elem[0]
            # query = """
            #     SHOW INDEX FROM %s
            #     """ % table_name
            # result = self.dbsrc_executor.dbfetchall(query, None)
        if tables_result is None:
            pass
        else:
            for i in tables_result:
                # 剔除主键
                if 'PRIMARY' in i[2]:
                    pass
                else:
                    if i[1] == 0:
                        dataset.append(
                            (self.keys, self.dbtype, self.dbname, i[0], 'UNIQUE', i[2], i[4], i[3], i[10]))
                    elif i[1] == 1:
                        dataset.append(
                            (self.keys, self.dbtype, self.dbname, i[0], 'NONUNIQUE', i[2], i[4], i[3], i[10]))
                    else:
                        mdtool.log.error("索引数据异常")
                        sys.exit()
        # 写入dbsyncer管理库 - mysql
        sql = """
                INSERT INTO tables_indexes_tabdb
                    (auth_id,
                    db_type,
                    table_schema,
                    table_name,
                    uniqueness,
                    index_name,
                    column_name,
                    ordinal_position,
                    index_type) 
                VALUES
                   (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s索引数据数据加载到mdsyncer库表tables_indexes_tabdb成功" % self.dbtype)


if __name__ == '__main__':
    dbsrc = mdtool.xmler('MYSQL_172.21.86.205').dbCFGInfo()
    dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    dialect = MysqlDialect(dbsrc, dbmgr)
    dialect.mdsyncer_tables()
    dialect.mdsyncer_columns()
    dialect.tables_constraints()
    dialect.tables_indexes()
