# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 17:19
# @Author  : Fcvane
# @Param   : 
# @File    : dialects_postgresql.py
import mdtool
import sys


class PGDialect():
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
            if value.get('schema'):
                self.schema = value['schema']
            else:
                self.schema = mdtool.Variable.PG_SCHEMA
                mdtool.log.warn("%s 源库缺失schema" % self.keys)
        # 调用工具类
        self.dbsrc_executor = mdtool.DbManager(self.host, self.port, self.user, self.passwd, self.dbname, self.dbtype,
                                               self.schema)
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
        # schema 改成从conf数据库配置文件中填写，如果不填，默认为public
        # # schema获取
        # query = """
        # SHOW SEARCH_PATH
        # """
        # result = self.dbsrc_executor.dbfetchone(query, None)
        # # self.dbschema = result[0]
        # self.dbschema = self.dbname.lower()

    # 表信息
    def mdsyncer_tables(self):
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_tables_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys),
            None)
        params = (self.keys, self.dbtype, self.schema)
        query = """
        SELECT 
            %s,
            %s,
            pt.schemaname,
            pt.tablename,
            obj_description(relfilenode, 'pg_class')
            FROM pg_tables pt,
                pg_class pc
            WHERE pt.tablename = pc.relname
                AND pt.schemaname = %s
        """
        dataset = self.dbsrc_executor.dbfetchall(query, params)

        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO mdsyncer_tables_tabdb
            (auth_id,
            db_type,
            table_schema,
            table_name,
            table_comment) 
        VALUES
            (%s, %s, %s, %s, %s)
        """
        # 加载数据前先删除历史记录
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s表信息数据加载到mdsyncer库表mdsyncer_tables_tabdb成功" % self.dbtype)

    # 字段信息
    def mdsyncer_columns(self):
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_columns_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys),
            None)
        params = (self.keys, self.dbtype, self.schema)
        query = """
        SELECT 
            %s,
            %s,
            col.table_schema,
            col.table_name,
            col.column_name,
            col.ordinal_position,
            col.column_default,
            CASE WHEN
                col.is_nullable = 'YES' THEN 'Y'
            ELSE
                'N' END is_nullable,
            col.data_type,
            col.character_maximum_length,
            col.numeric_precision,
            col.numeric_scale,
            t.format_type,
            t.col_description
        FROM INFORMATION_SCHEMA.columns col
        JOIN (
            SELECT pt.schemaname,
                pt.tablename,
                pa.attname,
                concat_ws('', pty.typname, SUBSTRING(format_type(pa.atttypid, pa.atttypmod) FROM '\(.*\)')),
                format_type(pa.atttypid, pa.atttypmod),
                col_description(pa.attrelid, pa.attnum)
            FROM pg_tables pt,
                pg_class pc,
                pg_attribute pa,
                pg_type pty
            WHERE pt.tablename = pc.relname
                AND pa.attrelid = pc.oid
                AND pa.atttypid = pty.oid
                AND pa.attnum > 0
                AND pt.schemaname = %s
            ) t ON col.table_schema = t.schemaname
            AND col.table_name = t.tablename
            AND col.column_name = t.attname
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
            column_type,
            column_comment) 
        VALUES
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s字段信息数据加载到mdsyncer库表mdsyncer_columns_tabdb成功" % self.dbtype)

    # 约束信息
    def tables_constraints(self):
        self.dbmgr_executor.dbexecute(
            "delete from tables_constraints_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys),
            None)
        sql = 'select count(1) from information_schema.referential_constraints'
        flag = self.dbsrc_executor.dbfetchall(sql, None)
        if flag[0][0] == 0:
            mdtool.log.warning('postgresql不存在约束')
        else:
            params = (self.keys, self.dbtype, self.schema)
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
                rc.table_name referenced_table_name,
                rc.column_name referenced_column_name,
                cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.table_name = kcu.table_name
                AND tc.table_schema = kcu.table_schema
                AND tc.constraint_name = kcu.constraint_name
            LEFT JOIN (
                SELECT rc.constraint_name,
                    kcu.table_name,
                    array_to_string(array_agg(kcu.column_name ORDER BY kcu.ordinal_position), ',') column_name
                FROM information_schema.referential_constraints rc
                JOIN information_schema.key_column_usage kcu ON rc.unique_constraint_name = kcu.constraint_name
                    AND rc.unique_constraint_schema = kcu.constraint_schema
                GROUP BY rc.constraint_name,
                    kcu.table_name
                ) rc ON rc.constraint_name = kcu.constraint_name
            LEFT JOIN information_schema.constraint_column_usage ccu ON kcu.constraint_name = ccu.constraint_name
                AND kcu.table_name = ccu.table_name
                AND kcu.table_schema = ccu.table_schema
                AND kcu.column_name = ccu.column_name
            LEFT JOIN information_schema.check_constraints cc ON tc.constraint_name = cc.constraint_name
            WHERE tc.table_schema = %s 
            """
            result = self.dbsrc_executor.dbfetchall(query, params)
            dataset = []
            # 判断CHECK是否为非空判断
            for elem in result:
                if 'IS NOT NULL' in str(elem[-1]):
                    pass
                else:
                    dataset.append(elem)
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
                check_condition) 
            VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
            self.dbmgr_executor.dbexecutemany(sql, dataset)
            mdtool.log.info("%s约束信息数据加载到mdsyncer库表tables_constraints_tabdb成功" % self.dbtype)

    # 索引信息
    def tables_indexes(self):
        self.dbmgr_executor.dbexecute(
            "delete from tables_indexes_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = (self.keys, self.dbtype, self.schema)
        query = """
        SELECT 
            %s,
            %s,
            idx.schemaname,
            idx.tablename,
            CASE 
                WHEN pidx.indisunique = 'true'
                    THEN 'UNIQUE'
                ELSE 'NONUNIQUE'
                END uniqueness,
            idx.indexname,
            pa.amname,
            idx.indexdef
        FROM PG_INDEXES idx
        JOIN PG_STAT_ALL_INDEXES sai ON idx.schemaname = sai.schemaname
            AND idx.tablename = sai.relname
            AND idx.indexname = sai.indexrelname
        JOIN PG_INDEX pidx ON sai.indexrelid = pidx.indexrelid
        JOIN PG_CLASS pc ON pidx.indexrelid = pc.oid
        JOIN PG_AM pa ON pc.relam = pa.oid
        WHERE idx.schemaname = %s
            AND pidx.indisprimary = 'false'
        """
        dataset = self.dbsrc_executor.dbfetchall(query, params)
        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO tables_indexes_tabdb
            (auth_id,
            db_type,
            table_schema,
            table_name,
            uniqueness,
            index_name,
            index_type,
            indexdef) 
        VALUES
           (%s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s索引数据数据加载到mdsyncer库表tables_indexes_tabdb成功" % self.dbtype)


if __name__ == '__main__':
    dbsrc = mdtool.xmler('POSTGRESQL_10.45.59.202').dbCFGInfo()
    dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    dialect = PGDialect(dbsrc, dbmgr)
    dialect.mdsyncer_tables()
    dialect.mdsyncer_columns()
    dialect.tables_constraints()
    dialect.tables_indexes()
