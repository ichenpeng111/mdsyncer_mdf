# -*- coding: utf-8 -*-
# @Time    : 2020/9/18 9:45
# @Author  : Fcvane
# @Param   : 
# @File    : profilegenerator.py
import mdtool
import os, sys, time


class ProfileGenerator():
    def __init__(self, dbsrc, dbtag, tables_in):
        self.dbsrc = dbsrc
        self.dbtag = dbtag
        self.tables_in = tables_in
        # self.tables_in = 'TMP_METRIC_GATHER_AREA_0628'
        # 源
        # key
        for key in dbsrc.keys():
            self.key = key
        for value in dbsrc.values():
            self.host = value['host']
            self.port = value['port']
            self.user = value['user']
            self.passwd = value['passwd']
            self.dbname = value['dbname']
            self.dbtype = value['dbtype'].lower()
            self.schema = None
            if self.dbtype == 'postgresql':
                if value.get('schema'):
                    self.schema = value['schema']
                else:
                    self.schema = mdtool.Variable.PG_SCHEMA
                    mdtool.log.warn("%s 管理库缺失schema" % self.key)

        # 目标
        # key
        for key in dbtag.keys():
            self.key_tag = key
        #value
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
                    mdtool.log.warn("%s 管理库缺失schema" % self.key_tag)

        self.dbsrc_executor = mdtool.DbManager(self.host, self.port, self.user, self.passwd, self.dbname, self.dbtype,
                                               self.schema)
        self.dbtag_executor = mdtool.DbManager(self.host_tag, self.port_tag, self.user_tag, self.passwd_tag,
                                               self.dbname_tag, self.dbtype_tag, self.schema_tag)

        self.JSON_PATH = mdtool.Variable.JSON_PATH
        self.JOB_PATH = mdtool.Variable.JOB_PATH

    def SedJson(self, src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table):
        # 模板扩展
        # shutil.copyfile(self.JSON_PATH + os.sep + '%s2%s.json' % (src, tag),
        #                 self.JOB_PATH + os.sep + '%s2%s_%s.json' % (src, tag, table))
        # 模板替换
        lines = open(self.JSON_PATH + os.sep + '%s2%s.json' % (src, tag))
        with open(self.JOB_PATH + os.sep + '%s2%s_%s.json' % (src, tag, table.upper()), 'w') as f:
            for line in lines:
                if 'SRC' in line:
                    f.write(
                        line.replace('SRCUSERNAME', self.user).
                            replace('SRCPASSWORD', self.passwd).
                            replace('SRCJDBC', src_jdbc).
                            replace('SRCSQL', src_sql)
                    )
                elif 'TAG' in line:
                    f.write(
                        line.replace('TAGUSERNAME', self.user_tag).
                            replace('TAGPASSWORD', self.passwd_tag).
                            replace('TAGCOL', tag_col).
                            replace('TAGSQL', tag_sql).
                            replace('TAGJDBC', tag_jdbc).
                            replace('TAGTABNAME', table)
                    )
                else:
                    f.write(line)
        f.close()

    def Profilejson(self):
        # mysql保留字
        mysql_reserved_words = ["add", "all", "alter", "analyze", "and", "as", "asc", "asensitive", "before", "between",
                                "bigint", "binary", "blob", "both", "by", "call", "cascade", "case", "change", "char",
                                "character", "check", "collate", "column", "condition", "connection", "constraint",
                                "continue", "convert", "create", "cross", "current_date", "current_time",
                                "current_timestamp", "current_user", "cursor", "database", "databases", "day_hour",
                                "day_microsecond", "day_minute", "day_second", "dec", "decimal", "declare", "default",
                                "delayed", "delete", "desc", "describe", "deterministic", "distinct", "distinctrow",
                                "div", "double", "drop", "dual", "each", "else", "elseif", "enclosed", "escaped",
                                "exists", "exit", "explain", "false", "fetch", "float", "float4", "float8", "for",
                                "force", "foreign", "from", "fulltext", "goto", "grant", "group", "having",
                                "high_priority", "hour_microsecond", "hour_minute", "hour_second", "if", "ignore", "in",
                                "index", "infile", "inner", "inout", "insensitive", "insert", "int", "int1", "int2",
                                "int3", "int4", "int8", "integer", "interval", "into", "is", "iterate", "join", "key",
                                "keys", "kill", "label", "leading", "leave", "left", "like", "limit", "linear", "lines",
                                "load", "localtime", "localtimestamp", "lock", "long", "longblob", "longtext", "loop",
                                "low_priority", "match", "mediumblob", "mediumint", "mediumtext", "middleint",
                                "minute_microsecond", "minute_second", "mod", "modifies", "natural", "not",
                                "no_write_to_binlog", "null", "numeric", "on", "optimize", "option", "optionally", "or",
                                "order", "out", "outer", "outfile", "precision", "primary", "procedure", "purge",
                                "raid0", "range", "read", "reads", "real", "references", "regexp", "release", "rename",
                                "repeat", "replace", "require", "restrict", "return", "revoke", "right", "rlike",
                                "schema", "schemas", "second_microsecond", "select", "sensitive", "separator", "set",
                                "show", "smallint", "spatial", "specific", "sql", "sqlexception", "sqlstate",
                                "sqlwarning", "sql_big_result", "sql_calc_found_rows", "sql_small_result", "ssl",
                                "starting", "straight_join", "table", "terminated", "then", "tinyblob", "tinyint",
                                "tinytext", "to", "trailing", "trigger", "true", "undo", "union", "unique", "unlock",
                                "unsigned", "update", "usage", "use", "using", "utc_date", "utc_time", "utc_timestamp",
                                "values", "varbinary", "varchar", "varcharacter", "varying", "when", "where", "while",
                                "with", "write", "x509", "xor", "year_month", "zerofill"]
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
            , "update"
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
                              "unique", "update", "alues", "view", "views", "when", "where", "with",
                              "alias", "prior", "comment"
                              ]
        if self.dbtype == 'oracle':
            src_jdbc = 'jdbc:oracle:thin:@%s:%s:%s' % (self.host, self.port, self.dbname)
            src = 'Ora'
            for table_elem in self.tables_in:
                table = table_elem.lower()
                params = {'table_name': table}
                query = """
                SELECT count(1) FROM USER_TABLES WHERE lower(table_name) = :table_name
                """
                result = self.dbsrc_executor.dbfetchone(query, params)
                mdtool.log.info("判断%s是存在于源以及目标数据库" % table)
                if result[0] != 1:
                    mdtool.log.warning("%s在%s数据库中不存在，请检查" % (table, self.dbtype))
                    # sys.exit()
                else:
                    mdtool.log.info("%s在%s源数据库中存在，继续检查表是否存在%s目标数据库"
                                    % (table, self.dbtype, self.dbtype_tag))
                    if self.dbtype_tag == 'mysql' or self.dbtype_tag == 'postgresql':
                        if self.dbtype_tag == 'mysql':
                            tag = 'MySQL'
                        else:
                            tag = 'PG'
                        tag_jdbc = 'jdbc:%s://%s:%s/%s?useUnicode=true&characterEncoding=utf8' % (
                            self.dbtype_tag, self.host_tag, self.port_tag, self.dbname_tag)
                        params = (table, self.dbname_tag.lower() if self.dbtype_tag == 'mysql' else self.schema_tag.lower())
                        query = """
                        SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = %s and table_schema = %s
                        """
                        result_tag = self.dbtag_executor.dbfetchone(query, params)
                        if result_tag[0] != 1:
                            mdtool.log.warning("%s在%s源数据库中存在，但是，在%s目标数据库不存在，请检查"
                                               % (table, self.dbtype, self.dbtype_tag))
                            # sys.exit()
                        else:
                            mdtool.log.info("%s在%s源数据库中存在，同时，也存在%s目标数据库"
                                            % (table, self.dbtype, self.dbtype_tag))
                            # 以目标库表字段为标准进行json配置
                            query = """
                            SELECT column_name FROM information_schema.columns
                            WHERE table_name = %s
                              and table_schema = %s 
                            ORDER BY ordinal_position
                            """
                            col_rst = self.dbtag_executor.dbfetchall(query, params)
                            col_array_tag = []
                            col_array_src = []
                            for elem in col_rst:
                                column_name = elem[0]
                                # 目标库保留字处理
                                if self.dbtype_tag == 'mysql':
                                    if column_name.lower() in set(mysql_reserved_words):
                                        column_name_tag = '`%s`' % column_name
                                    else:
                                        column_name_tag = column_name
                                    col_array_tag.append(column_name_tag)
                                else:
                                    if column_name.lower() in set(pg_reserved_words):
                                        column_name_tag = '\\"%s\\"' % column_name
                                    else:
                                        column_name_tag = column_name
                                    col_array_tag.append(column_name_tag)
                                # 源库保留字处理
                                if column_name.lower() in set(ora_reserved_words):
                                    column_name_src = '\\"%s\\"' % column_name.upper()
                                else:
                                    column_name_src = column_name
                                col_array_src.append(column_name_src)
                            tag_col = '","'.join(col_array_tag)
                            src_col = ','.join(col_array_src)
                            src_sql = 'SELECT %s FROM %s' % (src_col, table)
                            tag_sql = 'delete from %s' % table
                            self.SedJson(src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table)
                    elif self.dbtype_tag == 'oracle':
                        tag_jdbc = 'jdbc:oracle:thin:@%s:%s:%s' % (self.host_tag, self.port_tag, self.dbname_tag)
                        tag = 'Ora'
                        params = {'table_name': table}
                        query = """
                        SELECT count(1) FROM user_tables WHERE lower(table_name) = :table_name
                        """
                        result_tag = self.dbtag_executor.dbfetchone(query, params)
                        if result_tag[0] != 1:
                            mdtool.log.warning("%s在%s源数据库中存在，但是，在%s目标数据库不存在，请检查"
                                               % (table, self.dbtype, self.dbtype_tag))
                            # sys.exit()
                        else:
                            mdtool.log.info("%s在%s源数据库中存在，同时，也存在%s目标数据库"
                                            % (table, self.dbtype, self.dbtype_tag))
                            # 以目标库表字段为标准进行json配置
                            params = {'table_name': table}
                            query = """
                            SELECT column_name FROM user_tab_cols
                            WHERE lower(table_name) = :table_name
                            ORDER BY column_id
                            """
                            col_rst = self.dbtag_executor.dbfetchall(query, params)
                            col_array_tag = []
                            col_array_src = []
                            for elem in col_rst:
                                column_name = elem[0]
                                # 目标库字段处理
                                column_name_tag = column_name
                                col_array_tag.append(column_name_tag)
                                # 源库保留字处理
                                if column_name.lower() in set(ora_reserved_words):
                                    column_name_src = '\\"%s\\"' % column_name.upper()
                                else:
                                    column_name_src = column_name
                                col_array_src.append(column_name_src)
                            tag_col = '","'.join(col_array_tag)
                            src_col = ','.join(col_array_src)
                            src_sql = 'SELECT %s FROM %s' % (src_col, table)
                            tag_sql = 'TRUNCATE TABLE %s' % table
                            self.SedJson(src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table)
        elif self.dbtype == 'mysql' or self.dbtype == 'postgresql':
            src_jdbc = 'jdbc:%s://%s:%s/%s?useUnicode=true&characterEncoding=utf8' % (
                self.dbtype, self.host, self.port, self.dbname)
            if self.dbtype == 'mysql':
                src = 'MySQL'
                reserved_words = mysql_reserved_words
            else:
                src = 'PG'
                reserved_words = pg_reserved_words
            for table_elem in self.tables_in:
                table = table_elem.lower()
                params = (table, self.dbname.lower() if self.dbtype == 'mysql' else self.schema.lower())
                query = """
                SELECT count(1) FROM information_schema.tables WHERE table_name = %s and table_schema = %s
                """
                result = self.dbsrc_executor.dbfetchone(query, params)
                mdtool.log.info("判断%s是存在于源以及目标数据库" % table)
                if result[0] != 1:
                    mdtool.log.warning("%s在%s数据库中不存在，请检查" % (table, self.dbtype))
                else:
                    mdtool.log.info("%s在%s源数据库中存在，继续检查表是否存在%s目标数据库"
                                    % (table, self.dbtype, self.dbtype_tag))
                    if self.dbtype_tag == 'oracle':
                        tag_jdbc = 'jdbc:oracle:thin:@%s:%s:%s' % (self.host_tag, self.port_tag, self.dbname_tag)
                        params = {'table_name': table}
                        query = """
                                    SELECT count(1) FROM user_tables WHERE lower(table_name) = :table_name
                                    """
                        result_tag = self.dbtag_executor.dbfetchone(query, params)
                        if result_tag[0] != 1:
                            mdtool.log.warning("%s在%s源数据库中存在，但是，在%s目标数据库不存在，请检查"
                                               % (table, self.dbtype, self.dbtype_tag))
                            # sys.exit()
                        else:
                            mdtool.log.info("%s在%s源数据库中存在，同时，也存在%s目标数据库"
                                            % (table, self.dbtype, self.dbtype_tag))
                            # 以目标库表字段为标准进行json配置
                            params = {'table_name': table}
                            query = """
                                                        SELECT column_name FROM user_tab_cols
                                                        WHERE lower(table_name) = :table_name
                                                        ORDER BY column_id
                                                        """
                            col_rst = self.dbtag_executor.dbfetchall(query, params)
                            col_array_tag = []
                            col_array_src = []
                            tag = 'Ora'
                            for elem in col_rst:
                                column_name = elem[0]
                                # 目标库字段处理
                                column_name_tag = column_name
                                col_array_tag.append(column_name_tag)
                                # 源库保留字处理
                                if column_name.lower() in set(reserved_words):
                                    column_name_src = '\\"%s\\"' % column_name.upper()
                                else:
                                    column_name_src = column_name
                                col_array_src.append(column_name_src)
                            tag_col = '","'.join(col_array_tag)
                            src_col = ','.join(col_array_src)
                            src_sql = 'SELECT %s FROM %s' % (src_col, table)
                            tag_sql = 'TRUNCATE TABLE %s' % table
                            self.SedJson(src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table)
                    elif self.dbtype_tag == 'mysql' or self.dbtype_tag == 'postgresql':
                        tag_jdbc = 'jdbc:%s://%s:%s/%s?useUnicode=true&characterEncoding=utf8' % (
                            self.dbtype_tag, self.host_tag, self.port_tag, self.dbname_tag)
                        params = (table, self.dbname_tag.lower() if self.dbtype_tag == 'mysql' else self.schema_tag.lower())
                        query = """
                                SELECT count(1) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = %s and table_schema = %s
                                """
                        result_tag = self.dbtag_executor.dbfetchone(query, params)
                        if result_tag[0] != 1:
                            mdtool.log.warning("%s在%s源数据库中存在，但是，在%s目标数据库不存在，请检查"
                                               % (table, self.dbtype, self.dbtype_tag))
                            # sys.exit()
                        else:
                            mdtool.log.info("%s在%s源数据库中存在，同时，也存在%s目标数据库"
                                            % (table, self.dbtype, self.dbtype_tag))
                            # 以目标库表字段为标准进行json配置
                            query = """
                                    SELECT column_name FROM information_schema.columns
                                    WHERE table_name = %s 
                                      and table_schema = %s
                                    ORDER BY ordinal_position
                                    """
                            col_rst = self.dbtag_executor.dbfetchall(query, params)
                            col_array_tag = []
                            col_array_src = []
                            tag = ''
                            for elem in col_rst:
                                column_name = elem[0]
                                # 目标库保留字处理
                                if self.dbtype_tag == 'mysql':
                                    tag = 'MySQL'
                                    if column_name.lower() in set(mysql_reserved_words):
                                        column_name_tag = '`%s`' % column_name
                                    else:
                                        column_name_tag = column_name
                                    col_array_tag.append(column_name_tag)
                                else:
                                    tag = 'PG'
                                    if column_name.lower() in set(pg_reserved_words):
                                        column_name_tag = '\\"%s\\"' % column_name
                                    else:
                                        column_name_tag = column_name
                                    col_array_tag.append(column_name_tag)
                                # 源库保留字处理
                                if column_name.lower() in set(reserved_words):
                                    column_name_src = '\\"%s\\"' % column_name.upper()
                                else:
                                    column_name_src = column_name
                                col_array_src.append(column_name_src)
                            tag_col = '","'.join(col_array_tag)
                            src_col = ','.join(col_array_src)
                            src_sql = 'SELECT %s FROM %s' % (src_col, table)
                            tag_sql = 'DELETE FROM %s' % table
                            self.SedJson(src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table)
                    elif self.dbtype_tag == 'oracle':
                        tag_jdbc = 'jdbc:oracle:thin:@%s:%s:%s' % (
                            self.host_tag, self.port_tag, self.dbname_tag)
                        tag = 'Ora'
                        params = {'table_name': table}
                        query = """
                                SELECT count(1) FROM user_tables WHERE lower(table_name) = :table_name
                                """
                        result_tag = self.dbtag_executor.dbfetchone(query, params)
                        if result_tag[0] != 1:
                            mdtool.log.warning("%s在%s源数据库中存在，但是，在%s目标数据库不存在，请检查"
                                               % (table, self.dbtype, self.dbtype_tag))
                            # sys.exit()
                        else:
                            mdtool.log.info("%s在%s源数据库中存在，同时，也存在%s目标数据库"
                                            % (table, self.dbtype, self.dbtype_tag))
                            # 以目标库表字段为标准进行json配置
                            params = {'table_name': table}
                            query = """
                                    SELECT column_name FROM user_tab_cols
                                    WHERE lower(table_name) = :table_name
                                    ORDER BY column_id
                                    """
                            col_rst = self.dbtag_executor.dbfetchall(query, params)
                            col_array_tag = []
                            col_array_src = []
                            for elem in col_rst:
                                column_name = elem[0]
                                # 目标库字段处理
                                column_name_tag = column_name
                                col_array_tag.append(column_name_tag)
                                # 源库保留字处理
                                if column_name.lower() in set(ora_reserved_words):
                                    column_name_src = '\\"%s\\"' % column_name.upper()
                                else:
                                    column_name_src = column_name
                                col_array_src.append(column_name_src)
                            tag_col = '","'.join(col_array_tag)
                            src_col = ','.join(col_array_src)
                            src_sql = 'SELECT %s FROM %s' % (src_col, table)
                            tag_sql = 'TRUNCATE TABLE %s' % table
                            self.SedJson(src, src_jdbc, src_sql, tag, tag_col, tag_sql, tag_jdbc, table)

    @staticmethod
    def DataxExecute(json_file):
        DATAX_BIN_PATH = mdtool.Variable.DATAX_BIN_PATH
        command = 'python %s' % DATAX_BIN_PATH + os.sep + 'datax.py' + ' %s' % json_file
        # os.system(command)
        # print('进程', os.getpid())
        # print('多进程开始处理====>>>>')
        # time.sleep(2)


if __name__ == '__main__':
    dbsrc = mdtool.xmler('ORACLE_172.21.86.201').dbCFGInfo()
    # dbtag = mdtool.xmler('POSTGRESQL_10.45.59.178_META').dbCFGInfo()
    dbtag = mdtool.xmler('POSTGRESQL_172.21.86.201').dbCFGInfo()
    tables_in = 'bfm_staff_his,metric_items,pub_restriction'.split(',')
    ps = ProfileGenerator(dbsrc, dbtag, tables_in)
    ps.Profilejson()
    # JOB_PATH = mdtool.Variable.JOB_PATH
    # for root, dirs, files in os.walk(JOB_PATH):
    #     # 遍历文件
    #     for f in files:
    #         file = os.path.join(root, f)
    #         ps.DataxExecute(file)
