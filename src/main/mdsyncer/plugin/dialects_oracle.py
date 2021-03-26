# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 17:19
# @Author  : Fcvane
# @Param   : 
# @File    : dialects_oracle.py

import mdtool
import os


class OracleDialect():
    def __init__(self, dbsrc, dbmgr, tables_in=None):
        self.tables_in = tables_in
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
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from mdsyncer_tables where db_type='%s'" % self.dbtype, None)
            params = {'dbtype': self.dbtype, 'dbname': self.user}
            query = """
            SELECT 
                :dbtype,
                :dbname,
                ut.table_name,
                utc.comments
            FROM user_tables ut
            LEFT JOIN user_tab_comments utc 
            ON ut.table_name=utc.table_name
            """
            dataset = self.dbsrc_executor.dbfetchall(query, params)
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute("delete from mdsyncer_tables where db_type='%s' and table_name='%s'" % (
                    self.dbtype, tab))
                params = {'dbtype': self.dbtype, 'dbname': self.user, 'tables_in': tab}
                query = """
                SELECT 
                    :dbtype,
                    :dbname,
                    ut.table_name,
                    utc.comments
                FROM user_tables ut, user_tab_comments utc 
                WHERE ut.table_name=utc.table_name
                AND lower(ut.table_name) in :tables_in
                """
                result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in result:
                    dataset.append(elem)
                # 表不存在写入日志
                if len(result) == 0:
                    mdtool.log.warning("%s表不存在于%s数据库,请检查" % (tab, self.dbtype))
        # 剔除空元素
        while '' in dataset:
            dataset.remove('')
        # 写入dbsyncer管理库 - mysql
        sql = """
        INSERT INTO mdsyncer_tables
            (db_type,
            table_schema,
            table_name,
            table_comment) 
        VALUES
            (%s, %s, %s, %s)
        """
        # 加载数据前先删除历史记录
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s表信息数据加载到mdsyncer库表mdsyncer_tables成功" % self.dbtype)

    # 字段信息
    def mdsyncer_columns(self):
        dataset = []
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from mdsyncer_columns where db_type='%s'" % self.dbtype, None)
            params = {'dbtype': self.dbtype, 'dbname': self.user}
            query = """
            SELECT 
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
        else:
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from mdsyncer_columns where db_type='%s' and table_name='%s'" % (
                        self.dbtype, tab), None)
                params = {'dbtype': self.dbtype, 'dbname': self.user, 'tables_in': tab}
                query = """
                SELECT 
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
                    user_col_comments ucc
                WHERE utc.table_name = ucc.table_name
                    AND utc.column_name = ucc.column_name
                    AND lower(utc.table_name) =:tables_in
                """
                try:
                    result = self.dbsrc_executor.dbfetchall(query, params)
                    for elem in result:
                        dataset.append(elem)
                except Exception as err:
                    mdtool.log.warning("字段注解乱码" + str(err))
                    query = """
                    SELECT 
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
                        user_col_comments ucc
                    WHERE utc.table_name = ucc.table_name
                        AND utc.column_name = ucc.column_name
                        AND lower(utc.table_name) = :tables_in
            """
                    result = self.dbsrc_executor.dbfetchall(query, params)
                    for elem in result:
                        dataset.append(elem)
        # 剔除空元素
        while '' in dataset:
            dataset.remove('')
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
            column_comment) 
        VALUES
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s字段信息数据加载到mdsyncer库表mdsyncer_columns成功" % self.dbtype)

    # 约束信息
    def tables_constraints(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from tables_constraints where db_type='%s'" % self.dbtype, None)
            params = {'dbtype': self.dbtype, 'dbname': self.user}
            query = """
            SELECT 
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
                    AND uc.table_name = ut.table_name
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
                result = self.dbsrc_executor.dbfetchall(query, params)
            except Exception as err:
                query = """
                SELECT 
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
                        AND uc.table_name = ut.table_name
                    ) t
                LEFT JOIN (
                    SELECT table_name,
                        constraint_name,
                        listagg(column_name,'') within group (order by position,table_name) column_name
                    FROM user_cons_columns
                    GROUP BY table_name,
                    constraint_name
                    ) ucc ON t.r_constraint_name = ucc.constraint_na
                   """
                result = self.dbsrc_executor.lbfetchall(query, params)
            dataset = []
            # 判断CHECK是否为非空判断
            for elem in result:
                if 'IS NOT NULL' in str(elem[-1]):
                    pass
                else:
                    db_type = elem[0]
                    table_schema = elem[1]
                    table_name = elem[2]
                    constraint_name = elem[3]
                    constraint_type = elem[4]
                    column_name = elem[5]
                    ordinal_position = elem[6]
                    referenced_table_name = elem[7]
                    if elem[8] is None:
                        referenced_column_name = elem[8]
                    else:
                        referenced_column_name = elem[8].read()
                    check_condition = elem[9]
                    dataset.append((db_type, table_schema, table_name, constraint_name, constraint_type, column_name,
                        ordinal_position, referenced_table_name, referenced_column_name, check_condition))
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from tables_constraints where db_type='%s' and table_name='%s'" % (self.dbtype, tab), None)
                params = {'dbtype': self.dbtype, 'dbname': self.user, 'tables_in': tab}
                query = """
                SELECT 
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
                        user_cons_columns ucc
                    WHERE uc.constraint_name = ucc.constraint_name
                        AND uc.table_name = ucc.table_name
                        AND lower(uc.table_name) = :tables_in
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
                    query = """
                    SELECT 
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
                            AND uc.table_name = ut.table_name
                            AND lower(uc.table_name) = :tables_in
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
                for elem in result:
                    if 'IS NOT NULL' in str(elem[-1]):
                        pass
                    else:
                        db_type = elem[0]
                        table_schema = elem[1]
                        table_name = elem[2]
                        constraint_name = elem[3]
                        constraint_type = elem[4]
                        column_name = elem[5]
                        ordinal_position = elem[6]
                        referenced_table_name = elem[7]
                        if elem[8] is None:
                            referenced_column_name = elem[8]
                        else:
                            referenced_column_name = elem[8].read()
                        check_condition = elem[9]
                        dataset.append((
                             db_type, table_schema, table_name, constraint_name, constraint_type, column_name,
                            ordinal_position, referenced_table_name, referenced_column_name, check_condition))
        # 剔除空元素
        while '' in dataset:
            dataset.remove('')
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
            check_condition) 
        VALUES
           (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           """
        self.dbmgr_executor.dbexecutemany(sql, dataset)
        mdtool.log.info("%s约束信息数据加载到mdsyncer库表tables_constraints成功" % self.dbtype)

    # 索引信息
    def tables_indexes(self):
        if self.tables_in is None:
            self.dbmgr_executor.dbexecute("delete from tables_indexes where db_type='%s'" % self.dbtype, None)
            params = {'dbtype': self.dbtype, 'dbname': self.user}
            query = """
            SELECT 
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
        else:
            dataset = []
            for tab in self.tables_in.split(','):
                self.dbmgr_executor.dbexecute(
                    "delete from tables_indexes where db_type='%s' and table_name='%s'" % (self.dbtype, tab), None)
                params = {'dbtype': self.dbtype, 'dbname': self.user, 'tables_in': tab}
                query = """
                SELECT 
                    :dbtype,
                    :dbname,
                    ui.table_name,
                    ui.uniqueness,
                    ui.index_name,
                    uic.column_name,
                    uic.column_position,
                    ui.index_type
                FROM user_indexes ui,
                    user_ind_columns uic
                WHERE ui.table_name = uic.table_name
                    AND ui.index_name = uic.index_name
                    AND lower(ui.table_name)=:tables_in
               """
                result = self.dbsrc_executor.dbfetchall(query, params)
                for elem in result:
                    dataset.append(elem)
        # 剔除空元素
        while '' in dataset:
            dataset.remove('')
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
    dbsrc = mdtool.xmler('ORACLE_10.45.59.187_HAINAN').dbCFGInfo()
    dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    # tables_in = 'pub_elec_label,pub_drop_item'
    array = []
    with open(mdtool.Variable.CONF_PATH + os.sep + 'RC_RES.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            array.append(line.strip('\n').lower())
    tables_in = ','.join(array)
    dialect = OracleDialect(dbsrc, dbmgr, tables_in)
    # dialect = OracleDialect(dbsrc, dbmgr)
    # dialect.mdsyncer_tables()
    # dialect.mdsyncer_columns()
    dialect.tables_constraints()
    # dialect.tables_indexes()
