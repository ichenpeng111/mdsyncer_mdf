# -*- coding: utf-8 -*-
# @Time    : 2020/9/15 15:12
# @Author  : Fcvane
# @Param   : 
# @File    : modeldatatype.py
import mdtool


class mdtype():
    def __init__(self, dbtype_src, dbtype_tag, column_name, column_default, is_nullable, data_type, character_length,
                 numeric_precision,
                 numeric_scale):
        self.dbtype_src = dbtype_src
        self.dbtype_tag = dbtype_tag
        self.column_name = column_name
        self.column_default = column_default
        self.is_nullable = is_nullable
        self.data_type = data_type.upper()
        self.character_length = character_length
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale

    def columntransform(self):
        if self.dbtype_src == 'oracle':
            if self.dbtype_tag == 'mysql':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值为空-字段可为空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值为空-字段可为空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "`%s` integer" % self.column_name
                                else:
                                    # column_type = "`%s` %s(%s,0)" % (
                                    #     self.column_name, 'decimal', self.character_length)
                                    column_type = "`%s` %s(38,0)" % (
                                        self.column_name, 'decimal')
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 3:
                                        column_type = "`%s` tinyint" % self.column_name
                                    elif self.numeric_precision <= 4:
                                        column_type = "`%s` smallint" % self.column_name
                                    elif self.numeric_precision <= 8:
                                        column_type = "`%s` integer" % self.column_name
                                    elif self.numeric_precision <= 19:
                                        column_type = "`%s` bigint" % self.column_name
                                    else:
                                        column_type = "`%s` decimal(%s)" % (self.column_name, self.numeric_precision)
                                elif self.numeric_scale is None:
                                    column_type = "`%s` double" % self.column_name
                                else:
                                    column_type = "`%s` %s(%s,%s)" % (
                                        self.column_name, 'decimal', self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度类型处理" % self.column_name)
                            column_type = "`%s` float" % self.column_name
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "`%s` longblob" % self.column_name
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s)" % (self.column_name, self.character_length)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "`%s` text" % self.column_name
                            else:
                                column_type = "`%s` varchar(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值为空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text" % self.column_name
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值为空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "`%s` datetime" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "`%s` text" % self.column_name
                    else:
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值为空-字段非空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值为空-字段非空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "`%s` integer not null" % self.column_name
                                else:
                                    # column_type = "`%s` %s(%s,0) not null" % (
                                    #     self.column_name, 'decimal', self.character_length)
                                    column_type = "`%s` %s(38,0) not null" % (
                                        self.column_name, 'decimal')
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 3:
                                        column_type = "`%s` tinyint not null" % self.column_name
                                    elif self.numeric_precision <= 4:
                                        column_type = "`%s` smallint not null" % self.column_name
                                    elif self.numeric_precision <= 8:
                                        column_type = "`%s` integer not null" % self.column_name
                                    elif self.numeric_precision <= 19:
                                        column_type = "`%s` bigint not null" % self.column_name
                                    else:
                                        column_type = "`%s` decimal(%s) not null" % (
                                            self.column_name, self.numeric_precision)
                                elif self.numeric_scale is None:
                                    column_type = "`%s` double not null" % self.column_name
                                else:
                                    column_type = "`%s` %s(%s,%s)  not null" % (
                                        self.column_name, 'decimal', self.numeric_precision, self.numeric_scale
                                    )
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度类型处理" % self.column_name)
                            column_type = "`%s` float not null" % self.column_name
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "`%s` longblob not null" % self.column_name
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s) not null" % (
                                self.column_name, self.character_length)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "`%s` text not null" % self.column_name
                            else:
                                column_type = "`%s` varchar(%s) not null" % (
                                    self.column_name, self.character_length)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值为空-字段非空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text not null" % self.column_name
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值为空-字段非空-时间类型处理" % self.column_name)
                            column_type = "`%s` datetime not null" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "`%s` text not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值非空-字段可为空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值非空-字段可为空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "`%s` integer default %s" % (self.column_name, self.column_default)
                                else:
                                    # column_type = "`%s` %s(%s,0) default %s" % (
                                    #     self.column_name, 'decimal', self.character_length, self.column_default)
                                    column_type = "`%s` %s(38,0) default %s" % (
                                        self.column_name, 'decimal', self.column_default)
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 3:
                                        column_type = "`%s` tinyint default %s" % (
                                            self.column_name, self.column_default)
                                    elif self.numeric_precision <= 4:
                                        column_type = "`%s` smallint default %s" % (
                                            self.column_name, self.column_default)
                                    elif self.numeric_precision <= 8:
                                        column_type = "`%s` integer default %s" % (
                                            self.column_name, self.column_default)
                                    elif self.numeric_precision <= 19:
                                        column_type = "`%s` bigint default %s" % (self.column_name, self.column_default)
                                    else:
                                        column_type = "`%s` decimal(%s) default %s" % (
                                            self.column_name, self.numeric_precision, self.column_default)
                                elif self.numeric_scale is None:
                                    column_type = "`%s` double default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "`%s` %s(%s,%s) default %s" % (
                                        self.column_name, 'decimal', self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度类型处理" % self.column_name)
                            column_type = "%s double precision default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "`%s` longblob default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                            else:
                                column_type = "`%s` varchar(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值非空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值非空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "`%s` timestamp default %s" % (self.column_name, 'CURRENT_TIMESTAMP')
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'NUMBER':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值类型处理" % self.column_name)
                                if self.numeric_precision is None:
                                    mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值类型字段精确缺失" % self.column_name)
                                    if self.numeric_scale == 0:
                                        column_type = "`%s` integer default %s" % (
                                            self.column_name, self.column_default)
                                    else:
                                        # column_type = "`%s` %s(%s,0) default %s" % (
                                        #     self.column_name, 'decimal', self.character_length, self.column_default)
                                        column_type = "`%s` %s(38,0) default %s" % (
                                            self.column_name, 'decimal', self.column_default)
                                else:
                                    if self.numeric_scale == 0:
                                        if self.numeric_precision <= 3:
                                            column_type = "`%s` tinyint default %s" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 4:
                                            column_type = "`%s` smallint default %s" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 8:
                                            column_type = "`%s` integer default %s" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 19:
                                            column_type = "`%s` bigint default %s" % (
                                                self.column_name, self.column_default)
                                        else:
                                            column_type = "`%s` decimal(%s) default %s" % (
                                                self.column_name, self.numeric_precision, self.column_default)
                                    elif self.numeric_scale is None:
                                        column_type = "`%s` double default %s" % (self.column_name, self.column_default)
                                    else:
                                        column_type = "`%s` %s(%s,%s) default %s" % (
                                            self.column_name, 'decimal', self.numeric_precision, self.numeric_scale,
                                            self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-可变精度类型处理" % self.column_name)
                                column_type = "`%s` float default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "`%s` longblob default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "`%s` char(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'VARCHAR' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "`%s` varchar(%s) default %s" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间类型处理" % self.column_name)
                                column_type = "`%s` timestamp default %s" % (self.column_name, 'CURRENT_TIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                        else:
                            if self.data_type == 'NUMBER':
                                mdtool.log.info("%s默认值非空-字段非空-数值类型处理" % self.column_name)
                                if self.numeric_precision is None:
                                    mdtool.log.info("%s默认值非空-字段非空-数值类型字段精确缺失" % self.column_name)
                                    if self.numeric_scale == 0:
                                        column_type = "`%s` integer default %s not null" % (
                                            self.column_name, self.column_default)
                                    else:
                                        # column_type = "`%s` %s(%s,0) default %s not null" % (
                                        #     self.column_name, 'decimal', self.character_length, self.column_default)
                                        column_type = "`%s` %s(38,0) default %s not null" % (
                                            self.column_name, 'decimal', self.column_default)
                                else:
                                    if self.numeric_scale == 0:
                                        if self.numeric_precision <= 3:
                                            column_type = "`%s` tinyint default %s not null" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 4:
                                            column_type = "`%s` smallint default %s not null" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 8:
                                            column_type = "`%s` integer default %s not null" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 19:
                                            column_type = "`%s` bigint default %s not null" % (
                                                self.column_name, self.column_default)
                                        else:
                                            column_type = "`%s` decimal(%s) default %s not null" % (
                                                self.column_name, self.numeric_precision, self.column_default)
                                    elif self.numeric_scale is None:
                                        column_type = "`%s` double default %s not null" % (
                                            self.column_name, self.column_default)
                                    else:
                                        column_type = "`%s` %s(%s,%s) default %s not null" % (
                                            self.column_name, 'decimal', self.numeric_precision, self.numeric_scale,
                                            self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度类型处理" % self.column_name)
                                column_type = "`%s` float default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "`%s` longblob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "`%s` char(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'VARCHAR' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "`%s` text default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "`%s` varchar(%s) default %s not null" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                                mdtool.log.info("%s默认值非空-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "`%s` text default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                                mdtool.log.info("%s默认值非空-字段非空-时间类型处理" % self.column_name)
                                column_type = "`%s` timestamp default %s not null" % (
                                    self.column_name, 'CURRENT_TIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "`%s` text default %s not null" % (self.column_name, self.column_default)
            elif self.dbtype_tag == 'postgresql':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值为空-字段可为空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值为空-字段可为空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s integer" % self.column_name
                                else:
                                    column_type = "%s %s(%s,0)" % (
                                        self.column_name, 'numeric', self.character_length)
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 4:
                                        column_type = "%s smallint" % self.column_name
                                    elif self.numeric_precision <= 8:
                                        column_type = "%s integer" % self.column_name
                                    elif self.numeric_precision <= 19:
                                        column_type = "%s bigint" % self.column_name
                                    else:
                                        column_type = "%s numeric(%s)" % (self.column_name, self.numeric_precision)
                                elif self.numeric_scale is None:
                                    column_type = "%s double precision" % self.column_name
                                else:
                                    column_type = "%s %s(%s,%s)" % (
                                        self.column_name, 'numeric', self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度类型处理" % self.column_name)
                            column_type = "%s double precision" % self.column_name
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea" % self.column_name
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s)" % (self.column_name, self.character_length)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text" % self.column_name
                            else:
                                column_type = "%s character varying(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值为空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s text" % self.column_name
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值为空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s text" % self.column_name
                    else:
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值为空-字段非空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值为空-字段非空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s integer not null" % self.column_name
                                else:
                                    column_type = "%s %s(%s,0) not null" % (
                                        self.column_name, 'numeric', self.character_length)
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 4:
                                        column_type = "%s smallint not null" % self.column_name
                                    elif self.numeric_precision <= 8:
                                        column_type = "%s integer not null" % self.column_name
                                    elif self.numeric_precision <= 19:
                                        column_type = "%s bigint not null" % self.column_name
                                    else:
                                        column_type = "%s numeric(%s) not null" % (
                                            self.column_name, self.numeric_precision)
                                elif self.numeric_scale is None:
                                    column_type = "%s double precision not null" % self.column_name
                                else:
                                    column_type = "%s %s(%s,%s)  not null" % (
                                        self.column_name, 'numeric', self.numeric_precision, self.numeric_scale
                                    )
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度类型处理" % self.column_name)
                            column_type = "%s double precision not null" % self.column_name
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea not null" % self.column_name
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s) not null" % (
                                self.column_name, self.character_length)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text not null" % self.column_name
                            else:
                                column_type = "%s character varying(%s) not null" % (
                                    self.column_name, self.character_length)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值为空-字段非空-CLOB类型处理" % self.column_name)
                            column_type = "%s text not null" % self.column_name
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值为空-字段非空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp not null" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "%s text not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMBER':
                            mdtool.log.info("%s默认值非空-字段可为空-数值类型处理" % self.column_name)
                            if self.numeric_precision is None:
                                mdtool.log.info("%s默认值非空-字段可为空-数值类型字段精确缺失" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s integer default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "%s %s(%s,0) default %s" % (
                                        self.column_name, 'numeric', self.character_length, self.column_default)
                            else:
                                if self.numeric_scale == 0:
                                    if self.numeric_precision <= 4:
                                        column_type = "%s smallint default %s" % (self.column_name, self.column_default)
                                    elif self.numeric_precision <= 8:
                                        column_type = "%s integer default %s" % (self.column_name, self.column_default)
                                    elif self.numeric_precision <= 19:
                                        column_type = "%s bigint default %s" % (self.column_name, self.column_default)
                                    else:
                                        column_type = "%s numeric(%s) default %s" % (
                                            self.column_name, self.numeric_precision, self.column_default)
                                elif self.numeric_scale is None:
                                    column_type = "%s double precision default %s" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s %s(%s,%s) default %s" % (
                                        self.column_name, 'numeric', self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度类型处理" % self.column_name)
                            column_type = "%s double precision default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif 'VARCHAR' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                            else:
                                column_type = "%s character varying(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                            mdtool.log.info("%s默认值非空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s text default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                            mdtool.log.info("%s默认值非空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp default %s" % (self.column_name, 'LOCALTIMESTAMP')
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s text default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'NUMBER':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值类型处理" % self.column_name)
                                if self.numeric_precision is None:
                                    mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值类型字段精确缺失" % self.column_name)
                                    if self.numeric_scale == 0:
                                        column_type = "%s integer default %s" % (self.column_name, self.column_default)
                                    else:
                                        column_type = "%s %s(%s,0) default %s" % (
                                            self.column_name, 'numeric', self.character_length, self.column_default)
                                else:
                                    if self.numeric_scale == 0:
                                        if self.numeric_precision <= 4:
                                            column_type = "%s smallint default %s" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 8:
                                            column_type = "%s integer default %s" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 19:
                                            column_type = "%s bigint default %s" % (
                                                self.column_name, self.column_default)
                                        else:
                                            column_type = "%s numeric(%s) default %s" % (
                                                self.column_name, self.numeric_precision, self.column_default)
                                    elif self.numeric_scale is None:
                                        column_type = "%s double precision default %s" % (
                                            self.column_name, self.column_default)
                                    else:
                                        column_type = "%s %s(%s,%s) default %s" % (
                                            self.column_name, 'numeric', self.numeric_precision, self.numeric_scale,
                                            self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-可变精度类型处理" % self.column_name)
                                column_type = "%s double precision default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s bytea default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s character(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'VARCHAR' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "%s text default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "%s character varying(%s) default %s" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'LOCALTIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                        else:
                            if self.data_type == 'NUMBER':
                                mdtool.log.info("%s默认值非空-字段非空-数值类型处理" % self.column_name)
                                if self.numeric_precision is None:
                                    mdtool.log.info("%s默认值非空-字段非空-数值类型字段精确缺失" % self.column_name)
                                    if self.numeric_scale == 0:
                                        column_type = "%s integer default %s not null" % (
                                            self.column_name, self.column_default)
                                    else:
                                        column_type = "%s %s(%s,0) default %s not null" % (
                                            self.column_name, 'numeric', self.character_length, self.column_default)
                                else:
                                    if self.numeric_scale == 0:
                                        if self.numeric_precision <= 4:
                                            column_type = "%s smallint default %s not null" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 8:
                                            column_type = "%s integer default %s not null" % (
                                                self.column_name, self.column_default)
                                        elif self.numeric_precision <= 19:
                                            column_type = "%s bigint default %s not null" % (
                                                self.column_name, self.column_default)
                                        else:
                                            column_type = "%s numeric(%s) default %s not null" % (
                                                self.column_name, self.numeric_precision, self.column_default)
                                    elif self.numeric_scale is None:
                                        column_type = "%s double precision default %s not null" % (
                                            self.column_name, self.column_default)
                                    else:
                                        column_type = "%s %s(%s,%s) default %s not null" % (
                                            self.column_name, 'numeric', self.numeric_precision, self.numeric_scale,
                                            self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度类型处理" % self.column_name)
                                column_type = "%s double precision default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'BLOB' or self.data_type == 'RAW' or self.data_type == 'LONG RAW' or self.data_type == 'BFILE':
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s bytea default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR' or self.data_type == 'NCHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s character(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'VARCHAR' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "%s text default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s character varying(%s) default %s not null" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CLOB' or self.data_type == 'NCLOB' or self.data_type == 'LONG' or self.data_type == 'UROWID' or self.data_type == 'ROWID':
                                mdtool.log.info("%s默认值非空-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s text default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATE' or self.data_type == 'TIMESTAMP(6)':
                                mdtool.log.info("%s默认值非空-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s timestamp default %s not null" % (
                                    self.column_name, 'LOCALTIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s text default %s not null" % (self.column_name, self.column_default)
        elif self.dbtype_src == 'mysql':
            if self.dbtype_tag == 'oracle':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值为空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s)" % (self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s number(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "%s number(%s)" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s float" % self.column_name
                        elif self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s number(%s)" % (self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s number(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s blob" % self.column_name
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s)" % (self.column_name, self.character_length)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob" % self.column_name
                        elif self.data_type == 'DATETIME':
                            mdtool.log.info("%s默认值为空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s date" % self.column_name
                        elif self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值为空-字段可为空-时间timestamp类型处理" % self.column_name)
                            column_type = "%s timestamp" % self.column_name
                        # 其他类型全部给varchar(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255)" % self.column_name
                    else:
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值为空-字段非空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s) not null" % (
                                    self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s number(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-整数类型处理" % self.column_name)
                            column_type = "%s number(%s) not null" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s float not null" % self.column_name
                        elif self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s number(%s) not null" % (
                                    self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s number(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "%s blob not null" % self.column_name
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s) not null" % (self.column_name, self.character_length)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值为空-字段非空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s) not null" % (self.column_name, self.character_length)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob not null" % self.column_name
                        elif self.data_type == 'DATETIME':
                            mdtool.log.info("%s默认值为空-字段非空-时间类型处理" % self.column_name)
                            column_type = "%s date not null" % self.column_name
                        elif self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值为空-字段非空-时间timestamp类型处理" % self.column_name)
                            column_type = "%s timestamp not null" % self.column_name
                        # 其他类型全部给varchar2(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255) not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值非空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            else:
                                column_type = "%s number(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale,
                                    self.column_default)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "%s number(%s) default %s" % (
                                self.column_name, self.numeric_precision, self.column_default)
                        elif self.data_type == 'FLOAT':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s float default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s number(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            else:
                                column_type = "%s number(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale,
                                    self.column_default)
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s blob default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'DATETIME':
                            mdtool.log.info("%s默认值非空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s date default %s" % (self.column_name, 'sysdate')
                        elif self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值非空-字段可为空-时间timestamp类型处理" % self.column_name)
                            column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                        # 其他类型全部给varchar2(255)
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255) default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s number(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'INT' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-整数类型处理" % self.column_name)
                                column_type = "%s number(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-可变精度Float类型处理" % self.column_name)
                                column_type = "%s float default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'DOUBLE':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-可变精度Double类型处理" % self.column_name)
                                if self.numeric_scale is None:
                                    column_type = "%s number(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'BLOB' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s blob default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s char(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'VARCHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                column_type = "%s varchar2(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'TEXT' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s clob default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATETIME':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s date default %s" % (self.column_name, 'sysdate')
                            elif self.data_type == 'TIMESTAMP':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                            # 其他类型全部给varchar2(255)
                            else:
                                mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s varchar2(255) default %s" % (self.column_name, self.column_default)
                        else:
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s number(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'INT' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-整数类型处理" % self.column_name)
                                column_type = "%s number(%s) default %s not null" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'FLOAT':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度Float类型处理" % self.column_name)
                                column_type = "%s float default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'DOUBLE':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度Double类型处理" % self.column_name)
                                if self.numeric_scale is None:
                                    column_type = "%s number(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'BLOB' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s blob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'CHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s char(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'VARCHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                column_type = "%s varchar2(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif 'TEXT' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s clob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'DATETIME':
                                mdtool.log.info("%s默认值非空-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s date default %s not null" % (
                                    self.column_name, 'sysdate')
                            elif self.data_type == 'TIMESTAMP':
                                mdtool.log.info("%s默认值非空-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                            # 其他类型全部给varchar2(255)
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s varchar2(255) default %s not null" % (
                                    self.column_name, self.column_default)
            elif self.dbtype_tag == 'postgresql':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值为空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s numeric(%s)" % (self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s numeric(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'SMALLINT' or self.data_type == 'INT' or self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值为空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "%s %s" % (self.column_name, self.data_type)
                        elif self.data_type == 'TINYINT':
                            mdtool.log.info("%s默认值为空-字段可为空-整数(小)类型处理" % self.column_name)
                            column_type = "%s numeric(%s)" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'FLOAT' or self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s double precision" % self.column_name
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea" % self.column_name
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text" % self.column_name
                            else:
                                column_type = "%s character varying(%s)" % (
                                    self.column_name, self.character_length)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-文本类型处理" % self.column_name)
                            column_type = "%s text" % self.column_name
                        elif self.data_type == 'DATETIME' or self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值为空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s text" % self.column_name
                    else:
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值为空-字段非空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s numeric(%s) not null" % (
                                    self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s numeric(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'SMALLINT' or self.data_type == 'INT' or self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值为空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "%s %s not null" % (self.column_name, self.data_type)
                        elif self.data_type == 'TINYINT':
                            mdtool.log.info("%s默认值为空-字段可为空-整数(小)类型处理" % self.column_name)
                            column_type = "%s numeric(%s) not null" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'FLOAT' or self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s double precision not null" % self.column_name
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea not null" % self.column_name
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s) not null" % (
                                self.column_name, self.character_length)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text" % self.column_name
                            else:
                                column_type = "%s character varying(%s) not null" % (
                                    self.column_name, self.character_length)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-文本类型处理" % self.column_name)
                            column_type = "%s text" % self.column_name
                        elif self.data_type == 'DATETIME' or self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值为空-字段非空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp not null" % self.column_name
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "%s text not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值非空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s numeric(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            else:
                                column_type = "%s numeric(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale, self.column_default)
                        elif self.data_type == 'SMALLINT' or self.data_type == 'INT' or self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值非空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "%s %s default %s" % (self.column_name, self.data_type, self.column_default)
                        elif self.data_type == 'TINYINT':
                            mdtool.log.info("%s默认值非空-字段可为空-整数(小)类型处理" % self.column_name)
                            column_type = "%s numeric(%s) default %s" % (
                                self.column_name, self.numeric_precision, self.column_default)
                        elif self.data_type == 'FLOAT' or self.data_type == 'DOUBLE':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度Float类型处理" % self.column_name)
                            column_type = "%s double precision default %s" % (self.column_name, self.column_default)
                        elif 'BLOB' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s bytea default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s character(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'VARCHAR':
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            if self.character_length > 2000:
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                            else:
                                column_type = "%s character varying(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                        elif 'TEXT' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-文本类型处理" % self.column_name)
                            column_type = "%s text default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'DATETIME' or self.data_type == 'TIMESTAMP':
                            mdtool.log.info("%s默认值非空-字段可为空-时间类型处理" % self.column_name)
                            column_type = "%s timestamp default %s" % (self.column_name, 'LOCALTIMESTAMP')
                        # 其他类型全部给text
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s text default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s numeric(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s numeric(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif self.data_type == 'SMALLINT' or self.data_type == 'INT' or self.data_type == 'BIGINT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-整数类型处理" % self.column_name)
                                column_type = "%s %s default %s" % (
                                    self.column_name, self.data_type, self.column_default)
                            elif self.data_type == 'TINYINT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-整数(小)类型处理" % self.column_name)
                                column_type = "%s numeric(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'FLOAT' or self.data_type == 'DOUBLE':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-可变精度Float类型处理" % self.column_name)
                                column_type = "%s double precision default %s" % (self.column_name, self.column_default)
                            elif 'BLOB' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s bytea default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s character(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'VARCHAR':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "%s text default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "%s character varying(%s) default %s" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif 'TEXT' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-文本类型处理" % self.column_name)
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATETIME' or self.data_type == 'TIMESTAMP':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'LOCALTIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s text default %s" % (self.column_name, self.column_default)
                        else:
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s numeric(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s numeric(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif self.data_type == 'SMALLINT' or self.data_type == 'INT' or self.data_type == 'BIGINT':
                                mdtool.log.info("%s默认值非空-字段非空-整数类型处理" % self.column_name)
                                column_type = "%s %s default %s not null" % (
                                    self.column_name, self.data_type, self.column_default)
                            elif self.data_type == 'TINYINT':
                                mdtool.log.info("%s默认值非空-字段非空-整数(小)类型处理" % self.column_name)
                                column_type = "%s numeric(%s) default %s not null" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'FLOAT' or self.data_type == 'DOUBLE':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度Float类型处理" % self.column_name)
                                column_type = "%s double precision default %s not null" % (
                                    self.column_name, self.column_default)
                            elif 'BLOB' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s bytea default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s character(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'VARCHAR':
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                if self.character_length > 2000:
                                    column_type = "%s text default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s character varying(%s) default %s not null" % (
                                        self.column_name, self.character_length, self.column_default)
                            elif 'TEXT' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-文本类型处理" % self.column_name)
                                column_type = "%s text default %s not null" % (self.column_name, self.column_default)
                            elif self.data_type == 'DATETIME' or self.data_type == 'TIMESTAMP':
                                mdtool.log.info("%s默认值非空-字段非空-时间类型处理" % self.column_name)
                                column_type = "%s timestamp default %s not null" % (self.column_name, 'LOCALTIMESTAMP')
                            # 其他类型全部给text
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s text default %s not null" % (self.column_name, self.column_default)
        elif self.dbtype_src == 'postgresql':
            if self.dbtype_tag == 'oracle':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMERIC':
                            mdtool.log.info("%s默认值为空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s)" % (self.column_name, self.numeric_precision)
                            elif self.numeric_scale is None:
                                column_type = "%s number" % (self.column_name)
                            else:
                                column_type = "%s number(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'INTEGER':
                            column_type = "%s number(10)" % self.column_name
                        elif self.data_type == 'SMALLINT':
                            column_type = "%s number(5)" % self.column_name
                        elif self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值为空-字段可为空-整数类型处理" % self.column_name)
                            if self.numeric_precision > 22:
                                mdtool.log.warning("%s默认值为空-字段可为空-整数类型处理-数值精度高于22，请检查" % self.column_name)
                                column_type = "%s number(22)" % (self.column_name)
                            else:
                                column_type = "%s number(%s)" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s float" % (self.column_name)
                            else:
                                column_type = "%s number(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s blob" % self.column_name
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值为空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob" % self.column_name
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            column_type = "%s timestamp" % self.column_name
                        # 其他类型全部给varchar(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255)" % self.column_name
                    else:
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值为空-字段非空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s) not null" % (
                                    self.column_name, self.numeric_precision)
                            else:
                                column_type = "%s number(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif self.data_type == 'INTEGER':
                            column_type = "%s number(10) not null" % self.column_name
                        elif self.data_type == 'SMALLINT':
                            column_type = "%s number(5) not null" % self.column_name
                        elif self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值为空-字段非空-整数类型处理" % self.column_name)
                            if self.numeric_precision > 22:
                                mdtool.log.warning("%s默认值为空-字段非空-整数类型处理-数值精度高于22，请检查" % self.column_name)
                                column_type = "%s number(22) not null" % (self.column_name)
                            else:
                                column_type = "%s number(%s) not null" % (self.column_name, self.numeric_precision)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s float not null" % (
                                    self.column_name)
                            else:
                                column_type = "%s number(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "%s blob not null" % self.column_name
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s) not null" % (self.column_name, self.character_length)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值为空-字段非空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s) not null" % (self.column_name, self.character_length)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值为空-字段非空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob not null" % self.column_name
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-时间timestamp类型处理" % self.column_name)
                            column_type = "%s timestamp not null" % self.column_name
                        # 其他类型全部给varchar2(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255) not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'DECIMAL':
                            mdtool.log.info("%s默认值非空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "%s number(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            else:
                                column_type = "%s number(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale,
                                    self.column_default)
                        elif self.data_type == 'INTEGER':
                            column_type = "%s number(10) default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'SMALLINT':
                            column_type = "%s number(5) default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'BIGINT':
                            mdtool.log.info("%s默认值非空-字段可为空-整数类型处理" % self.column_name)
                            if self.numeric_precision > 22:
                                mdtool.log.warning("%s默认值非空-字段可为空-整数类型处理-数值精度高于22，请检查" % self.column_name)
                                column_type = "%s number(22) default %s" % (self.column_name, self.column_default)
                            else:
                                column_type = "%s number(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度Double类型处理" % self.column_name)
                            if self.numeric_scale is None:
                                column_type = "%s float default %s" % (
                                    self.column_name, self.column_default)
                            else:
                                column_type = "%s number(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale,
                                    self.column_default)
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "%s blob default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "%s char(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "%s varchar2(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值非空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "%s clob default %s" % (self.column_name, self.column_default)
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-时间timestamp类型处理" % self.column_name)
                            column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                        # 其他类型全部给varchar2(255)
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "%s varchar2(255) default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s number(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif self.data_type == 'INTEGER':
                                column_type = "%s number(10) default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'SMALLINT':
                                column_type = "%s number(5) default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'BIGINT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-整数类型处理" % self.column_name)
                                if self.numeric_precision > 22:
                                    mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-整数类型处理-数值精度高于22，请检查" % self.column_name)
                                    column_type = "%s number(22) default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "%s number(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'DOUBLE PRECISION':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-可变精度Double类型处理" % self.column_name)
                                if self.numeric_scale is None:
                                    column_type = "%s float default %s" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'BYTEA' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s blob default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHARACTER':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s char(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CHARACTER VARYING':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                column_type = "%s varchar2(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'TEXT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s clob default %s" % (self.column_name, self.column_default)
                            elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                            # 其他类型全部给varchar2(255)
                            else:
                                mdtool.log.warning("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s varchar2(255) default %s" % (self.column_name, 'sysdate')
                        else:
                            if self.data_type == 'DECIMAL':
                                mdtool.log.info("%s默认值非空-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "%s number(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif self.data_type == 'INTEGER':
                                column_type = "%s number(10) default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'SMALLINT':
                                column_type = "%s number(5) default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'BIGINT':
                                mdtool.log.info("%s默认值非空-字段非空-整数类型处理" % self.column_name)
                                if self.numeric_precision > 22:
                                    mdtool.log.warning("%s默认值非空-字段非空-整数类型处理-数值精度高于22，请检查" % self.column_name)
                                    column_type = "%s number(22) default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s number(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                            elif self.data_type == 'DOUBLE PRECISION':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度Double类型处理" % self.column_name)
                                if self.numeric_scale is None:
                                    column_type = "%s float default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "%s number(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'BYTEA' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "%s blob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'CHARACTER':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "%s char(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CHARACTER VARYING':
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                column_type = "%s varchar2(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'TEXT':
                                mdtool.log.info("%s默认值非空-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "%s clob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "%s timestamp default %s" % (self.column_name, 'sysdate')
                            # 其他类型全部给varchar2(255)
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "%s varchar2(255) default %s not null" % (
                                    self.column_name, self.column_default)
            elif self.dbtype_tag == 'mysql':
                if self.column_default is None:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMERIC':
                            mdtool.log.info("%s默认值为空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "`%s` decimal(%s)" % (self.column_name, self.numeric_precision)
                            elif self.numeric_scale is None:
                                column_type = "`%s` decimal" % (self.column_name)
                            else:
                                column_type = "`%s` decimal(%s,%s)" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "`%s` %s(%s)" % (self.column_name, self.data_type, self.numeric_precision)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值为空-字段可为空-可变精度Double类型处理" % self.column_name)
                            column_type = "`%s` double" % self.column_name
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "`%s` blob" % self.column_name
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值为空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值为空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "`%s` varchar(%s)" % (self.column_name, self.character_length)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值为空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text" % self.column_name
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            column_type = "`%s` datetime" % self.column_name
                        # 其他类型全部给varchar(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "`%s` varchar(255)" % self.column_name
                    else:
                        if self.data_type == 'NUMERIC':
                            mdtool.log.info("%s默认值为空-字段非空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "`%s` decimal(%s) not null" % (
                                    self.column_name, self.numeric_precision)
                            elif self.numeric_scale is None:
                                column_type = "`%s` decimal not null" % (self.column_name)
                            else:
                                column_type = "`%s` decimal(%s,%s) not null" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-整数类型处理" % self.column_name)
                            column_type = "`%s` %s(%s) not null" % (
                                self.column_name, self.data_type, self.numeric_precision)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值为空-字段非空-可变精度Double类型处理" % self.column_name)
                            column_type = "`%s` double not null" % self.column_name
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-二进制类型处理" % self.column_name)
                            column_type = "`%s` blob not null" % self.column_name
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值为空-字段非空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s) not null" % (self.column_name, self.character_length)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值为空-字段非空-字符类型处理" % self.column_name)
                            column_type = "`%s` varchar(%s) not null" % (self.column_name, self.character_length)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值为空-字段非空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text not null" % self.column_name
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            mdtool.log.info("%s默认值为空-字段非空-时间timestamp类型处理" % self.column_name)
                            column_type = "`%s` datetime not null" % self.column_name
                        # 其他类型全部给varchar2(255)
                        else:
                            mdtool.log.warning("%s默认值为空-字段非空-其他类型处理" % self.column_name)
                            column_type = "`%s` varchar(255) not null" % self.column_name
                else:
                    if self.is_nullable == 'Y':
                        if self.data_type == 'NUMERIC':
                            mdtool.log.info("%s默认值非空-字段可为空-数值Decimal类型处理" % self.column_name)
                            if self.numeric_scale == 0:
                                column_type = "`%s` decimal(%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.column_default)
                            elif self.numeric_scale is None:
                                column_type = "`%s` decimal default %s" % (self.column_name, self.column_default)
                            else:
                                column_type = "`%s` decimal(%s,%s) default %s" % (
                                    self.column_name, self.numeric_precision, self.numeric_scale,
                                    self.column_default)
                        elif 'INT' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-整数类型处理" % self.column_name)
                            column_type = "`%s` %s(%s) default %s" % (
                                self.column_name, self.data_type, self.numeric_precision, self.column_default)
                        elif self.data_type == 'DOUBLE PRECISION':
                            mdtool.log.info("%s默认值非空-字段可为空-可变精度Double类型处理" % self.column_name)
                            column_type = "`%s` double default %s" % (self.column_name, self.column_default)
                        elif 'BYTEA' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-二进制类型处理" % self.column_name)
                            column_type = "`%s` blob default %s" % (self.column_name, self.column_default)
                        elif self.data_type == 'CHARACTER':
                            mdtool.log.info("%s默认值非空-字段可为空-字符定长类型处理" % self.column_name)
                            column_type = "`%s` char(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'CHARACTER VARYING':
                            mdtool.log.info("%s默认值非空-字段可为空-字符类型处理" % self.column_name)
                            column_type = "`%s` varchar(%s) default %s" % (
                                self.column_name, self.character_length, self.column_default)
                        elif self.data_type == 'TEXT':
                            mdtool.log.info("%s默认值非空-字段可为空-CLOB类型处理" % self.column_name)
                            column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                        elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                            mdtool.log.info("%s默认值非空-字段可为空-时间timestamp类型处理" % self.column_name)
                            column_type = "`%s` timestamp default %s" % (self.column_name, 'CURRENT_TIMESTAMP')
                        # 其他类型全部给varchar(255)
                        else:
                            mdtool.log.warning("%s默认值非空-字段可为空-其他类型处理" % self.column_name)
                            column_type = "`%s` varchar(255) default %s" % (self.column_name, self.column_default)
                    else:
                        if self.column_default.lower().strip(' ') == 'null':
                            if self.data_type == 'NUMERIC':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "`%s` decimal(%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                elif self.numeric_scale is None:
                                    column_type = "`%s` decimal default %s" % (self.column_name, self.column_default)
                                else:
                                    column_type = "`%s` decimal(%s,%s) default %s" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'INT' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-整数类型处理" % self.column_name)
                                column_type = "`%s` %s(%s) default %s" % (
                                    self.column_name, self.data_type, self.numeric_precision, self.column_default)
                            elif self.data_type == 'DOUBLE PRECISION':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-可变精度Double类型处理" % self.column_name)
                                column_type = "`%s` double default %s" % (self.column_name, self.column_default)
                            elif 'BYTEA' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-二进制类型处理" % self.column_name)
                                column_type = "`%s` blob default %s" % (self.column_name, self.column_default)
                            elif self.data_type == 'CHARACTER':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "`%s` char(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CHARACTER VARYING':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-字符类型处理" % self.column_name)
                                column_type = "`%s` varchar(%s) default %s" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'TEXT':
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "`%s` text default %s" % (self.column_name, self.column_default)
                            elif 'TIMESTAMP' in self.data_type or 'DATE' in self.data_type:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "`%s` timestamp default %s" % (self.column_name, 'CURRENT_TIMESTAMP')
                            # 其他类型全部给varchar(255)
                            else:
                                mdtool.log.info("%s默认值非空-默认值结果为null-字段非空-其他类型处理" % self.column_name)
                                column_type = "`%s` varchar(255) default %s" % (self.column_name)
                        else:
                            if self.data_type == 'NUMERIC':
                                mdtool.log.info("%s默认值非空-字段非空-数值Decimal类型处理" % self.column_name)
                                if self.numeric_scale == 0:
                                    column_type = "`%s` decimal(%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.column_default)
                                elif self.numeric_scale is None:
                                    column_type = "`%s` decimal default %s not null" % (
                                        self.column_name, self.column_default)
                                else:
                                    column_type = "`%s` decimal(%s,%s) default %s not null" % (
                                        self.column_name, self.numeric_precision, self.numeric_scale,
                                        self.column_default)
                            elif 'INT' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-整数类型处理" % self.column_name)
                                column_type = "`%s` %s(%s) default %s not null" % (
                                    self.column_name, self.data_type, self.numeric_precision, self.column_default)
                            elif self.data_type == 'DOUBLE PRECISION':
                                mdtool.log.info("%s默认值非空-字段非空-可变精度Double类型处理" % self.column_name)
                                column_type = "`%s` double default %s not null" % (
                                    self.column_name, self.column_default)
                            elif 'BYTEA' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-二进制类型处理" % self.column_name)
                                column_type = "`%s` blob default %s not null" % (
                                    self.column_name, self.column_default)
                            elif self.data_type == 'CHARACTER':
                                mdtool.log.info("%s默认值非空-字段非空-字符定长类型处理" % self.column_name)
                                column_type = "`%s` char(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'CHARACTER VARYING':
                                mdtool.log.info("%s默认值非空-字段非空-字符类型处理" % self.column_name)
                                column_type = "`%s` varchar(%s) default %s not null" % (
                                    self.column_name, self.character_length, self.column_default)
                            elif self.data_type == 'TEXT':
                                mdtool.log.info("%s默认值非空-字段非空-CLOB类型处理" % self.column_name)
                                column_type = "`%s` text default %s not null" % (
                                    self.column_name, self.column_default)
                            elif 'TIMESTAMP' in self.data_type  or 'DATE' in self.data_type:
                                mdtool.log.info("%s默认值非空-字段非空-时间timestamp类型处理" % self.column_name)
                                column_type = "`%s` timestamp default %s" % (self.column_name, 'CURRENT_TIMESTAMP')
                            # 其他类型全部给varchar(255)
                            else:
                                mdtool.log.warning("%s默认值非空-字段非空-其他类型处理" % self.column_name)
                                column_type = "`%s` varchar(255) default %s not null" % (
                                    self.column_name, self.column_default)
        return column_type
