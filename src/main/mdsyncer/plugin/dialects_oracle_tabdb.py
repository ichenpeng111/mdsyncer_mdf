# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 17:19
# @Author  : Fcvane
# @Param   : 
# @File    : dialects_oracle.py

import mdtool
import os


class OracleDialect():
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
        # 加载数据前先删除历史记录
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_tables_tabdb where db_type='%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = {'auth_id': self.keys, 'dbtype': self.dbtype, 'dbname': self.user}
        query = """
        SELECT 
            :auth_id,
            :dbtype,
            :dbname,
            ut.table_name,
            utc.comments
        FROM user_tables ut
        LEFT JOIN user_tab_comments utc 
        ON ut.table_name=utc.table_name
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
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s表信息数据加载到mdsyncer库表mdsyncer_tables_tabdb成功" % self.dbtype)

    # 字段信息
    def mdsyncer_columns(self):
        self.dbmgr_executor.dbexecute(
            "delete from mdsyncer_columns_tabdb where db_type='%s' and auth_id = '%s'" % (self.dbtype, self.keys), None)
        params = {'auth_id': self.keys, 'dbtype': self.dbtype, 'dbname': self.user}
        dataset = []
        query = """
        SELECT 
            :auth_id,
            :dbtype,
            :dbname,
            utc.table_name,
            utc.column_name,
            utc.column_id,
            utc.data_default,
            utc.nullable,
            utc.data_type,
            utc.data_length,
            utc.data_precision,
            utc.data_scale,
            ucc.comments
        FROM user_tab_columns utc,
            user_col_comments ucc,
            user_tables ut
        WHERE utc.table_name = ucc.table_name
            AND utc.column_name = ucc.column_name
            AND utc.table_name = ut.table_name
           """
        try:
            result = self.dbsrc_executor.dbfetchall(query, params)
            for elem in result:
                dataset.append(elem)
        except Exception as err:
            mdtool.log.warning("字段注解乱码" + str(err))
            query = """
            SELECT 
                :auth_id,
                :dbtype,
                :dbname,
                utc.table_name,
                utc.column_name,
                utc.column_id,
                utc.data_default,
                utc.nullable,
                utc.data_type,
                utc.data_length,
                utc.data_precision,
                utc.data_scale,
                '' comments
            FROM user_tab_columns utc,
                user_col_comments ucc,
                user_tables ut
            WHERE utc.table_name = ucc.table_name
            AND utc.column_name = ucc.column_name
            AND utc.table_name = ut.table_name
            """
            result = self.dbsrc_executor.dbfetchall(query, params)
            for elem in result:
                dataset.append(elem)
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
                column_comment) 
            VALUES
               (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s字段信息数据加载到mdsyncer库表mdsyncer_columns_tabdb成功" % self.dbtype)

    # 约束信息
    def tables_constraints(self):
        self.dbmgr_executor.dbexecute(
            "delete from tables_constraints_tabdb where db_type = '%s' and auth_id = '%s'" % (self.dbtype, self.keys),
            None)
        params = {'auth_id': self.keys, 'dbtype': self.dbtype, 'dbname': self.user}
        query = """
        SELECT 
            :auth_id,
            :dbtype,
            :dbname,
            t.table_name,
            t.constraint_name,
            t.constraint_type,
            t.column_name,
            t.position,
            ucc.table_name,
            ucc.column_name,
            t.search_condition
        FROM (
            SELECT uc.table_name,
                uc.constraint_name,
                CASE 
                    WHEN uc.constraint_type = 'R'
                        THEN 'FOREIGN KEY'
                    WHEN uc.constraint_type = 'U'
                        THEN 'UNIQUE'
                    WHEN uc.constraint_type = 'P'
                        THEN 'PRIMARY KEY'
                    WHEN uc.constraint_type = 'C'
                        THEN 'CHECK'
                    END constraint_type,
                ucc.column_name,
                ucc.position,
                uc.r_constraint_name,
                uc.search_condition
            FROM user_constraints uc,
                user_cons_columns ucc,
                user_tables ut
            WHERE uc.constraint_name = ucc.constraint_name
                AND uc.table_name = ucc.table_name
                AND uc.table_name =ut.table_name
            ) t
        LEFT JOIN (
            SELECT table_name,
                constraint_name,
                wm_concat(to_char(column_name)) column_name
            FROM (
                SELECT table_name,constraint_name,column_name
                FROM user_cons_columns
                ORDER BY position,
                    table_name
                )
            GROUP BY table_name,
                constraint_name
            ) ucc ON t.r_constraint_name = ucc.constraint_name
           """
        try:
            result = self.dbsrc_executor.lbfetchall(query, params)
        except Exception as err:
            # 数据库版本问题，函数不可用
            query = """
            SELECT 
                :auth_id,
                :dbtype,
                :dbname,
                t.table_name,
                t.constraint_name,
                t.constraint_type,
                t.column_name,
                t.position,
                ucc.table_name,
                ucc.column_name,
                t.search_condition
            FROM (
                SELECT uc.table_name,
                    uc.constraint_name,
                    CASE 
                        WHEN uc.constraint_type = 'R'
                            THEN 'FOREIGN KEY'
                        WHEN uc.constraint_type = 'U'
                            THEN 'UNIQUE'
                        WHEN uc.constraint_type = 'P'
                            THEN 'PRIMARY KEY'
                        WHEN uc.constraint_type = 'C'
                            THEN 'CHECK'
                        END constraint_type,
                    ucc.column_name,
                    ucc.position,
                    uc.r_constraint_name,
                    uc.search_condition
                FROM user_constraints uc,
                    user_cons_columns ucc,
                    user_tables ut
                WHERE uc.constraint_name = ucc.constraint_name
                    AND uc.table_name = ucc.table_name
                    AND uc.table_name =ut.table_name
                ) t
            LEFT JOIN (
                SELECT table_name,
                    constraint_name,
                    listagg(column_name,'') within group (order by position,table_name) column_name
                FROM user_cons_columns
                GROUP BY table_name,
                constraint_name
                ) ucc ON t.r_constraint_name = ucc.constraint_name
               """
            result = self.dbsrc_executor.lbfetchall(query, params)
        # 判断CHECK是否为非空判断
        dataset = []
        for elem in result:
            if 'IS NOT NULL' in str(elem[-1]):
                pass
            else:
                auth_id = elem[0]
                db_type = elem[1]
                table_schema = elem[2]
                table_name = elem[3]
                constraint_name = elem[4]
                constraint_type = elem[5]
                column_name = elem[6]
                ordinal_position = elem[7]
                referenced_table_name = elem[8]
                if elem[9] is None:
                    referenced_column_name = elem[9]
                else:
                    referenced_column_name = elem[9].read()
                check_condition = elem[10]
                dataset.append((
                    auth_id, db_type, table_schema, table_name, constraint_name, constraint_type, column_name,
                    ordinal_position, referenced_table_name, referenced_column_name, check_condition))
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
        params = {'auth_id': self.keys, 'dbtype': self.dbtype, 'dbname': self.user}
        query = """
        SELECT 
            :auth_id,
            :dbtype,
            :dbname,
            ui.table_name,
            ui.uniqueness,
            ui.index_name,
            uic.column_name,
            uic.column_position,
            ui.index_type
        FROM user_indexes ui,
            user_ind_columns uic,
            user_tables ut
        WHERE ui.table_name = uic.table_name
            AND ui.index_name = uic.index_name
            AND ui.table_name = ut.table_name
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
            column_name,
            ordinal_position,
            index_type) 
        VALUES
           (%s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s索引数据数据加载到mdsyncer库表tables_indexes_tabdb成功" % self.dbtype)


if __name__ == '__main__':
    dbsrc = mdtool.xmler('ORACLE_10.45.59.215').dbCFGInfo()
    dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    dialect = OracleDialect(dbsrc, dbmgr)
    # dialect.mdsyncer_tables()
    # dialect.mdsyncer_columns()
    dialect.tables_constraints()
    # dialect.tables_indexes()
