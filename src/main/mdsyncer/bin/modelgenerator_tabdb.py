# -*- coding: utf-8 -*-
# @Time    : 2020/9/7 15:26
# @Author  : Fcvane
# @Param   : 
# @File    : modelgenerator.py
import mdtool
# import sys
# sys.path.append('..')
import dialects_mysql_tabdb, dialects_oracle_tabdb, dialects_postgresql_tabdb
import os
import sys
import modeldatatype


# 为了避免配置文件表过多，多次遍历造成查询缓慢，故采用配置文件入库，通过表关联进行处理
# dbsrc - 模型需要转换的数据库信息
# dbmgr - dbsyncer工具管理数据库信息
# flag - 表对象、约束、索引等可选，以逗号为分隔符  格式：table,constraint,index
# dbtag - 模型转换后的数据库信息

class ModelDialect():
    def __init__(self, dbsrc, dbmgr, flag=None):
        self.dbsrc = dbsrc
        self.dbmgr = dbmgr
        self.flag = flag
        for value in dbsrc.values():
            self.dbtype = value['dbtype'].lower()

    def main(self):
        if 'table' in self.flag or 'constraint' in self.flag or 'index' in self.flag:
            result = self.flag.split(',')
            if self.dbtype == 'oracle':
                dialects = dialects_oracle_tabdb.OracleDialect(self.dbsrc, self.dbmgr)
                for elem in result:
                    if elem == 'table':
                        dialects.mdsyncer_tables()
                        dialects.mdsyncer_columns()
                    elif elem == 'constraint':
                        dialects.tables_constraints()
                    elif elem == 'index':
                        dialects.tables_indexes()
                    else:
                        mdtool.log.error("数据库对象参数错误，请重新处理")
            elif self.dbtype == 'mysql':
                dialects = dialects_mysql_tabdb.MysqlDialect(self.dbsrc, self.dbmgr)
                for elem in result:
                    if elem == 'table':
                        dialects.mdsyncer_tables()
                        dialects.mdsyncer_columns()
                    elif elem == 'constraint':
                        dialects.tables_constraints()
                    elif elem == 'index':
                        dialects.tables_indexes()
                    else:
                        mdtool.log.error("数据库对象参数错误，请重新处理")
            elif self.dbtype == 'postgresql':
                dialects = dialects_postgresql_tabdb.PGDialect(self.dbsrc, self.dbmgr)
                for elem in result:
                    if elem == 'table':
                        dialects.mdsyncer_tables()
                        dialects.mdsyncer_columns()
                    elif elem == 'constraint':
                        dialects.tables_constraints()
                    elif elem == 'index':
                        dialects.tables_indexes()
                    else:
                        mdtool.log.error("数据库对象参数错误，请重新处理")
        else:
            mdtool.log.error("数据库对象参数格式错误，请重新处理")


# 模型生成器
class Modelgenerator():
    def __init__(self, dbsrc, dbmgr, dbtag):
        self.dbsrc = dbsrc
        self.dbmgr = dbmgr
        self.dbtag = dbtag
        # keys
        for key in dbsrc.keys():
            self.keys = key
        # 源
        for value in dbsrc.values():
            self.host = value['host']
            self.port = value['port']
            self.user = value['user']
            self.passwd = value['passwd']
            self.dbtype = value['dbtype'].lower()
            self.dbname = value['dbname'].lower()
            self.schema = None
            if self.dbtype == 'postgresql':
                if value.get('schema'):
                    self.schema = value['schema']
                else:
                    self.schema = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 库缺失schema" % self.keys)
        # 源库
        self.dbsrc_executor = mdtool.DbManager(self.host, self.port, self.user, self.passwd,
                                               self.dbname, self.dbtype, self.schema)

        # 目标
        # key
        for key in dbtag.keys():
            self.keys_tag = key
        # value
        for value in dbtag.values():
            self.host_tag = value['host']
            self.port_tag = value['port']
            self.user_tag = value['user']
            self.passwd_tag = value['passwd']
            self.dbname_tag = value['dbname']
            self.dbtype_tag = value['dbtype'].lower()
            self.schema_tag = None
            if self.dbtype_tag == 'postgresql':
                if value.get('schema'):
                    self.schema_tag = value['schema']
                else:
                    self.schema_tag = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 库缺失schema" % self.keys_tag)
        # 目标库
        self.dbtag_executor = mdtool.DbManager(self.host_tag, self.port_tag, self.user_tag, self.passwd_tag,
                                               self.dbname_tag, self.dbtype_tag, self.schema_tag)
        # 管理库
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
            self.schema_mgr = None
            if self.dbtype_mgr == 'postgresql':
                if value.get('schema'):
                    self.schema_mgr = value['schema']
                else:
                    self.schema_mgr = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 库缺失schema" % self.keys_mgr)
        # 管理库
        self.dbmgr_executor = mdtool.DbManager(self.host_mgr, self.port_mgr, self.user_mgr, self.passwd_mgr,
                                               self.dbname_mgr, self.dbtype_mgr, self.schema_mgr)

        # 表对象文件处理
        self.filedir_tab = mdtool.Variable.RST_PATH + os.sep + 'tables_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)
        if not os.path.exists(self.filedir_tab):
            os.makedirs(self.filedir_tab)
        else:
            with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                      'w') as file:
                file.truncate(0)
            file.close()
        # 约束对象文件处理
        self.filedir_cst = mdtool.Variable.RST_PATH + os.sep + 'constraints_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)
        if not os.path.exists(self.filedir_cst):
            os.makedirs(self.filedir_cst)
        else:
            with open(self.filedir_cst + os.sep + '%s2%s_constraint_model.sql' % (
                    self.dbtype, self.dbtype_tag),
                      'w') as file:
                file.truncate(0)
            with open(self.filedir_cst + os.sep + '%s2%s_constraint_fk_model.sql' % (
                    self.dbtype, self.dbtype_tag),
                      'w') as file:
                file.truncate(0)
            file.close()
        # 索引对象文件处理
        self.filedir_idx = mdtool.Variable.RST_PATH + os.sep + 'indexes_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)
        if not os.path.exists(self.filedir_idx):
            os.makedirs(self.filedir_idx)
        else:
            with open(self.filedir_idx + os.sep + '%s2%s_index_model.sql' % (self.dbtype, self.dbtype_tag),
                      'w') as file:
                file.truncate(0)
            file.close()

    # 删除不需要同步的对象
    def DelObject(self):
        tables = "mdsyncer_tables_tabdb,mdsyncer_columns_tabdb,tables_constraints_tabdb,tables_indexes_tabdb"
        for table in tables.split(','):
            query = "DELETE mt FROM %s mt " \
                    "LEFT JOIN cfg_tables ct " \
                    "ON mt.auth_id = ct.auth_id " \
                    "AND lower(mt.table_name) = ct.table_name " \
                    "WHERE ct.table_name is null AND mt.auth_id = '%s'" % (
                        table, self.keys)
            self.dbmgr_executor.sql_execute(query)

    # 表对象生成
    def tablesGenerator(self):
        if self.dbtype == 'oracle':
            params = (self.dbtype, self.user, self.keys)
        elif self.dbtype == 'mysql':
            params = (self.dbtype, self.dbname, self.keys)
        elif self.dbtype == 'postgresql':
            params = (self.dbtype, self.schema, self.keys)
        query = """
        SELECT 
            lower(table_name) table_name
        FROM mdsyncer_tables_tabdb mt
        WHERE db_type = %s
        AND table_schema = %s
        AND auth_id = %s 
        ORDER BY table_name
        """
        tabnames = self.dbmgr_executor.dbfetchall(query, params)
        dataset = []
        # dataset_tab = []
        for tabname in tabnames:
            query = """
            SELECT 
                dbt.table_comment,
                dbc.column_name,
                dbc.column_default,
                dbc.is_nullable,
                dbc.data_type,
                dbc.character_length,
                dbc.numeric_precision,
                dbc.numeric_scale,
                dbc.column_comment
            FROM mdsyncer_tables_tabdb dbt,
                mdsyncer_columns_tabdb dbc
            WHERE dbt.db_type = dbc.db_type
                AND dbt.auth_id = dbc.auth_id
                AND dbt.table_schema = dbc.table_schema
                AND dbt.table_name = dbc.table_name
                AND dbt.db_type = %s
                AND dbt.table_name = %s
                AND dbt.auth_id = %s
            ORDER BY dbc.ordinal_position   
            """
            result = self.dbmgr_executor.dbfetchall(query, (self.dbtype, tabname[0], self.keys))
            # 异构数据类型转换
            array = []
            colcomm = []
            if self.dbtype == 'oracle':
                if self.dbtype_tag == 'mysql':
                    mdtool.log.info("%s2%s----------------------------%s表模型开始构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, tabname[0]))
                    for elem in result:
                        table_comment = elem[0]
                        column_name = elem[1]
                        column_default = elem[2]
                        is_nullable = elem[3]
                        data_type = elem[4]
                        character_length = elem[5]
                        numeric_precision = elem[6]
                        numeric_scale = elem[7]
                        column_comment = elem[8]
                        mt = modeldatatype.mdtype(self.dbtype, self.dbtype_tag, column_name, column_default,
                                                  is_nullable,
                                                  data_type,
                                                  character_length,
                                                  numeric_precision,
                                                  numeric_scale)
                        column_type = mt.columntransform()
                        # 字段生成器
                        array.append(column_type)
                        # 注释为空的不处理
                        if column_comment is None:
                            pass
                        else:
                            # 注意规避注解中单引号
                            colcomm.append("ALTER TABLE %s MODIFY COLUMN %s COMMENT '%s';" % (
                                tabname[0], column_type, column_comment.replace('\'', '').replace(';', ' ')))
                    if table_comment is None:
                        ddl_tables = "CREATE TABLE %s(%s) default charset=utf8 ;" % (
                            tabname[0], ','.join(array))
                    else:
                        ddl_tables = "CREATE TABLE %s(%s) default charset=utf8 comment = '%s';" % (
                            tabname[0], ','.join(array), table_comment.replace(';', ' '))
                    # 慎重选择，避免删除已存在的表
                    # dataset_tab.append("DROP TABLE IF EXISTS %s " % tabname[0])
                    dataset.append(ddl_tables)
                    dataset.append(''.join(colcomm))
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'a') as f:
                        f.write(''.join(dataset))
                    f.close()
                    # 初始化
                    dataset = []
                    dataset_tab = []
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型成功构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))
                elif self.dbtype_tag == 'postgresql':
                    # pg保留关键字
                    pg_reserved_words = ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
                                         "asymmetric", "both", "case", "cast", "check", "collate", "column",
                                         "constraint", "create", "current_catalog", "current_date",
                                         "current_role", "current_time", "current_timestamp", "current_user",
                                         "default", "deferrable", "desc", "distinct", "do", "else", "end",
                                         "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
                                         "having", "in", "initially", "intersect", "into", "leading", "limit",
                                         "localtime", "localtimestamp", "new", "not", "null", "of", "off",
                                         "offset", "old", "on", "only", "or", "order", "placing", "primary",
                                         "references", "returning", "select", "session_user", "some", "symmetric",
                                         "table", "then", "to", "trailing", "true", "union", "unique", "user",
                                         "using", "variadic", "when", "where", "window", "with", "authorization",
                                         "between", "binary", "cross", "current_schema", "freeze", "full",
                                         "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
                                         "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
                                         ]
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型开始构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))
                    for elem in result:
                        table_comment = elem[0]
                        column_name = elem[1]
                        if column_name.lower() in set(pg_reserved_words):
                            mdtool.log.warning("命名不规范：%s字段为PG保留关键字" % column_name)
                            column_name = '"%s"' % column_name
                        column_default = elem[2]
                        is_nullable = elem[3]
                        data_type = elem[4]
                        character_length = elem[5]
                        numeric_precision = elem[6]
                        numeric_scale = elem[7]
                        column_comment = elem[8]
                        mt = modeldatatype.mdtype(self.dbtype, self.dbtype_tag, column_name, column_default,
                                                  is_nullable,
                                                  data_type,
                                                  character_length,
                                                  numeric_precision,
                                                  numeric_scale)
                        column_type = mt.columntransform()
                        # 字段生成器
                        array.append(column_type)
                        # 注释为空的不处理
                        if column_comment is None:
                            pass
                        else:
                            # 注意规避注解中单引号
                            colcomm.append("COMMENT ON COLUMN %s.%s IS '%s';" % (
                                tabname[0], column_name, column_comment.replace('\'', '').replace(';', ' ')))
                    ddl_tables = "CREATE TABLE %s(%s);" % (
                        tabname[0], ','.join(array))
                    dataset.append(ddl_tables)
                    if table_comment is None:
                        pass
                    else:
                        dataset.append(
                            "COMMENT ON TABLE %s is '%s';" % (tabname[0], table_comment.replace(';', ' ')))
                    if len(colcomm) > 0:
                        dataset.append(''.join(colcomm))
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'a') as f:
                        f.write(''.join(dataset))
                    f.close()
                    # 初始化
                    dataset = []
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型成功构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))
            elif self.dbtype == 'mysql':
                if self.dbtype_tag == 'oracle' or self.dbtype_tag == 'postgresql':
                    # pg保留字
                    pg_reserved_words = ["all", "analyse", "analyze", "and", "any", "array", "as", "asc",
                                         "asymmetric", "both", "case", "cast", "check", "collate", "column",
                                         "constraint", "create", "current_catalog", "current_date",
                                         "current_role", "current_time", "current_timestamp", "current_user",
                                         "default", "deferrable", "desc", "distinct", "do", "else", "end",
                                         "except", "false", "fetch", "for", "foreign", "from", "grant", "group",
                                         "having", "in", "initially", "intersect", "into", "leading", "limit",
                                         "localtime", "localtimestamp", "new", "not", "null", "of", "off",
                                         "offset", "old", "on", "only", "or", "order", "placing", "primary",
                                         "references", "returning", "select", "session_user", "some", "symmetric",
                                         "table", "then", "to", "trailing", "true", "union", "unique", "user",
                                         "using", "variadic", "when", "where", "window", "with", "authorization",
                                         "between", "binary", "cross", "current_schema", "freeze", "full",
                                         "ilike", "inner", "is", "isnull", "join", "left", "like", "natural",
                                         "notnull", "outer", "over", "overlaps", "right", "similar", "verbose"
                                         ]
                    # oracle保留字
                    ora_reserved_words = ["all", "alter", "and", "any", "as", "asc", "at", "begin", "between", "by",
                                          "case", "check", "clusters", "cluster", "colauth", "columns", "compress",
                                          "connect",
                                          "crash", "create", "cursor", "declare", "default", "desc", "distinct",
                                          "drop", "else", "end", "exception", "exclusive", "fetch", "for", "from",
                                          "function", "goto", "grant", "group", "having", "identified", "if", "in",
                                          "index", "indexes", "insert", "intersect", "into", "is", "like", "lock",
                                          "minus",
                                          "mode", "nocompress", "not", "nowait", "null", "of", "on", "option", "or",
                                          "order", "overlaps", "procedure", "public", "resource", "revoke", "select",
                                          "share", "size", "sql", "start", "subtype", "tabauth", "table", "then", "to",
                                          "type", "union",
                                          "unique", "update", "alues", "view", "views", "when", "where", "with"
                                          ]
                    mdtool.log.info("%s2%s----------------------------%s表模型开始构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, tabname[0]))
                    for elem in result:
                        table_comment = elem[0]
                        column_name = elem[1]
                        if self.dbtype_tag == 'oracle':
                            if column_name.lower() in set(ora_reserved_words):
                                mdtool.log.warning("命名不规范：%s字段为Oracle保留字" % column_name)
                                column_name = '"%s"' % column_name
                        elif self.dbtype_tag == 'postgresql':
                            if column_name.lower() in set(pg_reserved_words):
                                mdtool.log.warning("命名不规范：%s字段为PG保留字" % column_name)
                                column_name = '"%s"' % column_name
                        column_default = elem[2]
                        is_nullable = elem[3]
                        data_type = elem[4].upper()
                        character_length = elem[5]
                        numeric_precision = elem[6]
                        numeric_scale = elem[7]
                        column_comment = elem[8]
                        mt = modeldatatype.mdtype(self.dbtype, self.dbtype_tag, column_name, column_default,
                                                  is_nullable,
                                                  data_type,
                                                  character_length,
                                                  numeric_precision,
                                                  numeric_scale)
                        column_type = mt.columntransform()
                        # 字段生成器
                        array.append(column_type)
                        # 注释为空的不处理
                        if column_comment is None or len(column_comment) == 0:
                            pass
                        else:
                            # 注意规避注解中单引号
                            colcomm.append("COMMENT ON COLUMN %s.%s IS '%s';" % (
                                tabname[0], column_name, column_comment.replace('\'', '').replace(';', ' ')))
                    ddl_tables = "CREATE TABLE %s(%s);" % (
                        tabname[0], ','.join(array))
                    dataset.append(ddl_tables)
                    if table_comment is None or len(table_comment) == 0:
                        pass
                    else:
                        dataset.append(
                            "COMMENT ON TABLE %s is '%s';" % (tabname[0], table_comment.replace(';', ' ')))
                    if len(colcomm) > 0:
                        dataset.append(''.join(colcomm))
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'a') as f:
                        f.write(''.join(dataset))
                    f.close()
                    # 初始化
                    dataset = []
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型成功构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))
            elif self.dbtype == 'postgresql':
                if self.dbtype_tag == 'mysql':
                    mdtool.log.info("%s2%s----------------------------%s表模型开始构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, tabname[0]))
                    for elem in result:
                        table_comment = elem[0]
                        column_name = elem[1]
                        column_default = elem[2]
                        is_nullable = elem[3]
                        data_type = elem[4].upper()
                        character_length = elem[5]
                        numeric_precision = elem[6]
                        numeric_scale = elem[7]
                        column_comment = elem[8]
                        # 默认值特殊处理
                        if 'CHARACTER' in data_type and column_default is not None:
                            column_default = column_default.split('::')[0]
                        mt = modeldatatype.mdtype(self.dbtype, self.dbtype_tag, column_name, column_default,
                                                  is_nullable,
                                                  data_type,
                                                  character_length,
                                                  numeric_precision,
                                                  numeric_scale)
                        column_type = mt.columntransform()
                        # 字段生成器
                        array.append(column_type)
                        # 注释为空的不处理
                        if column_comment is None:
                            pass
                        else:
                            # 注意规避注解中单引号
                            colcomm.append("ALTER TABLE %s MODIFY COLUMN %s COMMENT '%s';" % (
                                tabname[0], column_type, column_comment.replace('\'', '').replace(';', ' ')))
                    if table_comment is None:
                        ddl_tables = "CREATE TABLE %s(%s) default charset=utf8 ;" % (
                            tabname[0], ','.join(array))
                    else:
                        ddl_tables = "CREATE TABLE %s(%s) default charset=utf8 comment = '%s';" % (
                            tabname[0], ','.join(array), table_comment.replace(';', ' '))
                    # 慎重选择，避免删除已存在的表
                    # dataset_tab.append("DROP TABLE IF EXISTS %s " % tabname[0])
                    dataset.append(ddl_tables)
                    dataset.append(''.join(colcomm))
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'a') as f:
                        f.write(''.join(dataset))
                    f.close()
                    # 初始化
                    dataset = []
                    # dataset_tab = []
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型成功构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))
                elif self.dbtype_tag == 'oracle':
                    # oracle保留字
                    ora_reserved_words = ["all", "alter", "and", "any", "as", "asc", "at", "begin", "between", "by",
                                          "case", "check", "clusters", "cluster", "colauth", "columns", "compress",
                                          "connect",
                                          "crash", "create", "cursor", "declare", "default", "desc", "distinct",
                                          "drop", "else", "end", "exception", "exclusive", "fetch", "for", "from",
                                          "function", "goto", "grant", "group", "having", "identified", "if", "in",
                                          "index", "indexes", "insert", "intersect", "into", "is", "like", "lock",
                                          "minus",
                                          "mode", "nocompress", "not", "nowait", "null", "of", "on", "option", "or",
                                          "order", "overlaps", "procedure", "public", "resource", "revoke", "select",
                                          "share", "size", "sql", "start", "subtype", "tabauth", "table", "then", "to",
                                          "type", "union",
                                          "unique", "update", "alues", "view", "views", "when", "where", "with"
                                          ]
                    mdtool.log.info("%s2%s----------------------------%s表模型开始构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, tabname[0]))
                    for elem in result:
                        table_comment = elem[0]
                        column_name = elem[1]
                        if column_name.lower() in set(ora_reserved_words):
                            mdtool.log.warning("命名不规范：%s字段为Oracle保留字" % column_name)
                            column_name = '"%s"' % column_name
                        column_default = elem[2]
                        is_nullable = elem[3]
                        data_type = elem[4].upper()
                        character_length = elem[5]
                        numeric_precision = elem[6]
                        numeric_scale = elem[7]
                        column_comment = elem[8]
                        # 默认值特殊处理
                        if 'CHARACTER' in data_type and column_default is not None:
                            column_default = column_default.split('::')[0]
                        mt = modeldatatype.mdtype(self.dbtype, self.dbtype_tag, column_name, column_default,
                                                  is_nullable,
                                                  data_type,
                                                  character_length,
                                                  numeric_precision,
                                                  numeric_scale)
                        column_type = mt.columntransform()
                        # 字段生成器
                        array.append(column_type)
                        # 注释为空的不处理
                        if column_comment is None:
                            pass
                        else:
                            # 注意规避注解中单引号
                            colcomm.append("COMMENT ON COLUMN %s.%s IS '%s';" % (
                                tabname[0], column_name, column_comment.replace('\'', '').replace(';', ' ')))
                    ddl_tables = "CREATE TABLE %s(%s);" % (
                        tabname[0], ','.join(array))
                    dataset.append(ddl_tables)
                    if table_comment is None:
                        pass
                    else:
                        dataset.append(
                            "COMMENT ON TABLE %s is '%s';" % (tabname[0], table_comment.replace(';', ' ')))
                    if len(colcomm) > 0:
                        dataset.append(''.join(colcomm))
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'a') as f:
                        f.write(''.join(dataset))
                    f.close()
                    # 初始化
                    dataset = []
                    mdtool.log.info(
                        "%s2%s----------------------------%s表模型成功构建----------------------------" % (
                            self.dbtype, self.dbtype_tag, tabname[0]))

    # 约束生成
    def constraintsGenerator(self):
        if self.dbtype == 'oracle' or self.dbtype == 'postgresql':
            params = (self.dbtype, self.keys)
            query = """
                SELECT 
                    table_name, 
                    constraint_type,
                    constraint_name,
                    group_concat(column_name ORDER BY ordinal_position),
                    referenced_table_name,
                    referenced_column_name,
                    check_condition
                FROM tables_constraints_tabdb
                WHERE db_type = %s
                AND auth_id = %s
                GROUP BY table_name,
                    constraint_type,
                    constraint_name,
                    referenced_table_name,
                    referenced_column_name,
                    check_condition
            """
            result = self.dbmgr_executor.dbfetchall(query, params)
        elif self.dbtype == 'mysql':
            params = (self.dbtype, self.keys)
            query = """
                SELECT 
                    table_name,
                    constraint_type,
                    CASE WHEN constraint_name='PRIMARY' 
                        THEN CONCAT('pk_',table_name)  
                    ELSE constraint_name END constraint_name,
                    group_concat(column_name ORDER BY ordinal_position),
                    referenced_table_name,
                    group_concat(referenced_column_name ORDER BY referenced_ordinal_position),
                    check_condition
                FROM tables_constraints_tabdb
                WHERE db_type = %s
                AND auth_id = %s
                GROUP BY table_name,
                    constraint_type,
                    constraint_name,
                    referenced_table_name,
                    check_condition
                """
            result = self.dbmgr_executor.dbfetchall(query, params)
        dataset = []
        # 外键最后执行处理
        dataset_fk = []
        for elem in result:
            constraint_sql = ''
            table_name = elem[0]
            constraint_type = elem[1]
            constraint_name = elem[2]
            column_name = elem[3]
            referenced_table_name = elem[4]
            referenced_column_name = elem[5]
            check_condition = elem[6]
            mdtool.log.info(
                "%s2%s----------------------------%s表_%s约束模型开始构建----------------------------" % (
                    self.dbtype, self.dbtype_tag, table_name, constraint_name))
            if constraint_type == 'PRIMARY KEY':
                mdtool.log.info("%s主键约束构建" % constraint_name)
                constraint_sql = "ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY(%s)" % (
                    table_name, constraint_name, column_name)
            elif constraint_type == 'FOREIGN KEY':
                mdtool.log.info("%s外键约束构建" % constraint_name)
                constraint_sql_fk = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)" % (
                    table_name, constraint_name, column_name, referenced_table_name, referenced_column_name)
                dataset_fk.append(constraint_sql_fk)
            elif constraint_type == 'CHECK':
                mdtool.log.info("%s检查约束构建" % constraint_name)
                constraint_sql = "ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)" % (
                    table_name, constraint_name, check_condition)
            # oracle主键会产生唯一性约束
            elif constraint_type == 'UNIQUE':
                if self.dbtag == 'oracle':
                    query = """
                    SELECT COUNT(1)
                    FROM (
                        SELECT table_name,
                            constraint_type,
                            group_concat(column_name ORDER BY ordinal_position) column_name
                        FROM tables_constraints_tabdb
                        WHERE db_type = %s
                        GROUP BY table_name,
                            constraint_type
                        ) t
                    WHERE table_name = %s
                        AND column_name = %s
                        AND constraint_type = 'PRIMARY KEY'
                    """
                    result_up = self.dbmgr_executor.dbfetchone(query, (self.dbtype, table_name, column_name))
                    if result_up[0] > 0:
                        pass
                    else:
                        mdtool.log.info("%s唯一性约束构建" % constraint_name)
                        constraint_sql = "ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)" % (
                            table_name, constraint_name, column_name)
                elif self.dbtag == 'mysql' or self.dbtag == 'postgresql':
                    mdtool.log.info("%s唯一性约束构建" % constraint_name)
                    constraint_sql = "ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)" % (
                        table_name, constraint_name, column_name)
            else:
                mdtool.log.error("%s_%s_%s约束类型错误" % (self.dbtype, table_name, constraint_name))
                sys.exit()
            mdtool.log.info(
                "%s2%s----------------------------%s表_%s约束模型成功构建----------------------------" % (
                    self.dbtype, self.dbtype_tag, table_name, constraint_name))
            dataset.append(constraint_sql)
            # constraint_sql = ""
        # 剔除空元素
        while '' in dataset:
            dataset.remove('')
        while '' in dataset_fk:
            dataset_fk.remove('')
        with open(self.filedir_cst + os.sep + '%s2%s_constraint_model.sql' % (self.dbtype, self.dbtype_tag), 'a') as f:
            f.write(';'.join(dataset))
        f.close()
        if len(dataset_fk) > 0:
            with open(self.filedir_cst + os.sep + '%s2%s_constraint_fk_model.sql' % (self.dbtype, self.dbtype_tag),
                      'a') as f:
                f.write(';'.join(dataset_fk))
            f.close()

    # 索引生成
    # 需要剔除主键约束产生的唯一性索引
    def indexesGenerator(self):
        if self.dbtype == 'oracle' or self.dbtype == 'mysql':
            params = (self.dbtype, self.keys)
            query = """
            SELECT 
                table_name,
                uniqueness,
                index_name,
                group_concat(column_name ORDER BY ordinal_position)
            FROM tables_indexes_tabdb ti
            WHERE db_type = %s
            AND auth_id = %s
                AND index_name NOT IN (
                    SELECT constraint_name
                    FROM tables_constraints_tabdb tc
                    WHERE ti.db_type = tc.db_type
                    AND ti.auth_id = tc.auth_id
                    )
            GROUP BY table_name,
                uniqueness,
                index_name
            """
            result = self.dbmgr_executor.dbfetchall(query, params)
            dataset = []
            for elem in result:
                table_name = elem[0]
                uniqueness = elem[1]
                index_name = elem[2]
                column_name = elem[3]
                mdtool.log.info(
                    "%s2%s----------------------------%s表_%s索引模型开始构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, table_name, index_name))
                if uniqueness == 'NONUNIQUE':
                    mdtool.log.info("%s普通索引构建" % index_name)
                    index_sql = "CREATE INDEX %s ON %s (%s)" % (index_name, table_name, column_name)
                else:
                    mdtool.log.info("%s唯一性索引构建" % index_name)
                    index_sql = "CREATE %s INDEX %s ON %s (%s)" % (uniqueness, index_name, table_name, column_name)
                mdtool.log.info(
                    "%s2%s----------------------------%s表_%s索引模型成功构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, table_name, index_name))
                dataset.append(index_sql)
            while '' in dataset:
                dataset.remove('')
            with open(self.filedir_idx + os.sep + '%s2%s_index_model.sql' % (self.dbtype, self.dbtype_tag), 'a') as f:
                f.write(';'.join(dataset))
            f.close()
        elif self.dbtype == 'postgresql':
            query = """
            SELECT 
                table_name,
                index_name,
                indexdef
            FROM tables_indexes_tabdb
            WHERE db_type = %s
            AND auth_id = %s
            """
            result = self.dbmgr_executor.dbfetchall(query, (self.dbtype, self.keys))
            dataset = []
            for elem in result:
                table_name = elem[0]
                index_name = elem[1]
                indexdef = elem[2]
                mdtool.log.info(
                    "%s2%s----------------------------%s表_%s索引模型成功构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, table_name, index_name))
                index_sql = indexdef.replace('%s.%s USING btree ' % (self.dbname, table_name), table_name)
                dataset.append(index_sql)
                mdtool.log.info(
                    "%s2%s----------------------------%s表_%s索引模型成功构建----------------------------" % (
                        self.dbtype, self.dbtype_tag, table_name, index_name))
            while '' in dataset:
                dataset.remove('')
            with open(self.filedir_idx + os.sep + '%s2%s_index_model.sql' % (self.dbtype, self.dbtype_tag), 'a') as f:
                f.write(';'.join(dataset))
            f.close()


# 加载进度条
from tqdm._tqdm import trange


# 模型加载到目标库
class Modeltodb():
    def __init__(self, dbsrc, dbtag, flag):
        self.dbsrc = dbsrc
        self.dbtag = dbtag
        self.flag = flag
        for value in dbsrc.values():
            self.dbtype = value['dbtype'].lower()
        # 目标
        # key
        for key in dbtag.keys():
            self.key_tag = key
        # value
        for value in dbtag.values():
            self.host_tag = value['host']
            self.port_tag = value['port']
            self.user_tag = value['user']
            self.passwd_tag = value['passwd']
            self.dbname_tag = value['dbname']
            self.dbtype_tag = value['dbtype'].lower()
            self.schema_tag = None
            if self.dbtype_tag == 'postgresql':
                if value.get('schema'):
                    self.schema_tag = value['schema']
                else:
                    self.schema_tag = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 库缺失schema" % self.key_tag)

        self.dbtag_executor = mdtool.DbManager(self.host_tag, self.port_tag, self.user_tag, self.passwd_tag,
                                               self.dbname_tag, self.dbtype_tag, self.schema_tag)

        self.filedir_tab = mdtool.Variable.RST_PATH + os.sep + 'tables_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)
        self.filedir_cst = mdtool.Variable.RST_PATH + os.sep + 'constraints_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)
        self.filedir_idx = mdtool.Variable.RST_PATH + os.sep + 'indexes_generator' + os.sep + '%s2%s' % (
            self.dbtype, self.dbtype_tag)

    def modelToDB(self):
        # 读取文件
        if 'table' in self.flag or 'constraint' in self.flag or 'index' in self.flag:
            result = self.flag.split(',')
            for object in result:
                # 表对象
                if object == 'table':
                    with open(self.filedir_tab + os.sep + '%s2%s_model.sql' % (self.dbtype, self.dbtype_tag),
                              'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    array_tab = ''.join(lines).split(';')
                    # 剔除空元素
                    while '' in array_tab:
                        array_tab.remove('')
                    if len(array_tab) > 0:
                        mdtool.log.info("表对象创建进度：")
                        for elem in trange(len(array_tab)):
                            elem_sql = array_tab[elem]
                            try:
                                self.dbtag_executor.sql_execute(elem_sql)
                                self.dbtag_executor.dbclose()
                            except Exception as err:
                                mdtool.log.error("失败的建表语句：%s;" % elem_sql)
                                mdtool.log.error("表对象创建失败：" + str(err))
                        f.close()
                    else:
                        mdtool.log.warning("模型生成器未生成表对象, 请检查是否正确")
                    mdtool.log.info("表对象创建完成")
                # 索引对象
                # 添加唯一性索引后可以再添加主键，故先执行索引对象
                elif object == 'index':
                    with open(self.filedir_idx + os.sep + '%s2%s_index_model.sql' % (self.dbtype, self.dbtype_tag),
                              'r') as f:
                        lines = f.readlines()
                    array_idx = ''.join(lines).split(';')
                    while '' in array_idx:
                        array_idx.remove('')
                    if len(array_idx) > 0:
                        mdtool.log.info("索引对象创建进度：")
                        for elem in trange(len(array_idx)):
                            elem_sql = array_idx[elem]
                            try:
                                self.dbtag_executor.sql_execute(elem_sql)
                                self.dbtag_executor.dbclose()
                            except Exception as err:
                                mdtool.log.error("失败的索引语句：%s;" % elem_sql)
                                mdtool.log.error("索引对象创建失败：" + str(err))
                        f.close()
                    else:
                        mdtool.log.warning("模型生成器未生成索引对象, 请检查是否正确")
                    mdtool.log.info("索引对象创建完成")
                # 约束对象
                elif object == 'constraint':
                    # 主键 、 检查 、 唯一性
                    with open(self.filedir_cst + os.sep + '%s2%s_constraint_model.sql' % (self.dbtype, self.dbtype_tag),
                              'r') as f:
                        lines = f.readlines()
                    array_cst = ''.join(lines).split(';')
                    while '' in array_cst:
                        array_cst.remove('')
                    if len(array_cst) > 0:
                        mdtool.log.info("约束对象创建进度：")
                        for elem in trange(len(array_cst)):
                            elem_sql = array_cst[elem]
                            try:
                                self.dbtag_executor.sql_execute(elem_sql)
                                self.dbtag_executor.dbclose()
                            except Exception as err:
                                mdtool.log.error("失败的约束语句：%s;" % elem_sql)
                                mdtool.log.error("约束对象创建失败：" + str(err))
                        f.close()
                    else:
                        mdtool.log.warning("模型生成器未生成约束对象, 请检查是否正确")
                    # 外键最后处理
                    with open(self.filedir_cst + os.sep + '%s2%s_constraint_fk_model.sql' % (
                            self.dbtype, self.dbtype_tag), 'r') as f:
                        lines = f.readlines()
                    array_fk = ''.join(lines).split(';')
                    while '' in array_fk:
                        array_fk.remove('')
                    if len(array_fk) > 0:
                        mdtool.log.info("约束-外键对象创建进度：")
                        for elem in trange(len(array_fk)):
                            elem_sql = array_fk[elem]
                            try:
                                self.dbtag_executor.sql_execute(elem_sql)
                                self.dbtag_executor.dbclose()
                            except Exception as err:
                                mdtool.log.error("失败的约束-外键语句：%s;" % elem_sql)
                                mdtool.log.error("约束-外键对象创建失败：" + str(err))
                        f.close()
                    else:
                        mdtool.log.warning("模型生成器未生成约束-外键对象, 请检查是否正确")
                    mdtool.log.info("约束(&外键)对象创建完成")


if __name__ == '__main__':
    # dbsrc = mdtool.xmler('MYSQL_172.21.86.205').dbCFGInfo()
    dbsrc = mdtool.xmler('ORACLE_10.45.59.187_HAINAN').dbCFGInfo()
    # dbtag = mdtool.xmler('ORACLE_172.21.86.201').dbCFGInfo()
    dbtag = mdtool.xmler('POSTGRESQL_10.45.59.178_META').dbCFGInfo()
    # flag = 'table,constraint,index'
    flag = 'table'
    # flag = 'constraint'
    # flag = 'index'
    # w = Modeltodb(dbsrc, dbtag, flag)
    # w.modelToDB()
    # tables = 'bfm_staff_org_his'
    # dbmgr = mdtool.xmler('MGR_172.21.86.205').dbCFGInfo()
    # Modelgenerator(dbsrc, dbmgr, dbtag, tables).tablesGenerator()
