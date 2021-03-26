# -*- coding: utf-8 -*-
# @Time    : 2020/9/14 8:30
# @Author  : Fcvane
# @Param   : 
# @File    : modelsyncer.py

import modelgenerator
import mdtool
import argparse
import os, sys


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--dbsrc', dest='dbsrc', required=True, help='模型需要转换的数据库信息')
    parser.add_argument('--dbtag', dest='dbtag', required=True, help='模型转换后的数据库信息')
    parser.add_argument('--dbmgr', dest='dbmgr', required=True, help='mbsyncer工具管理数据库信息')
    parser.add_argument('--flag', dest='flag', required=False,
                        help='表对象、约束、索引等可选，以逗号为分隔符 格式：table,constraint,index')
    parser.add_argument('--tables_in', dest='tables_in', required=False,
                        help='按表对象导出，以逗号为分隔符 格式：tab1,tab2,tab3')
    parser.add_argument('--dbin', dest='dbin', required=False,
                        help='是否直接加载到DB: Y生成模型文件且加载到DB;N生成模型文件不加载到DB；默认不直接加载')
    # parser.add_argument('--tabfile_flag', dest='tabfile_flag', required=False,
    #                     help='是否读取文件方式批量导出表对象: Y读取文件，N不用读取')
    # parser.add_argument('--tabfile', dest='tabfile', required=False, help='需要同步的表配置文件名称,配合tabfile_flag参数一起使用')

    args = parser.parse_args()
    dbsrc = mdtool.xmler(args.dbsrc).dbCFGInfo()
    dbtag = mdtool.xmler(args.dbtag).dbCFGInfo()
    dbmgr = mdtool.xmler(args.dbmgr).dbCFGInfo()
    # 对象处理
    # 默认导出 表\约束\索引对象
    if args.flag is None:
        flag = 'table,index,constraint'
    else:
        flag = args.flag.lower()
    # 表名处理
    # 批量导出表采用配置文件的方式，避免输入内容过多
    if args.tables_in is None:
        tables_in = None
    else:
        tables_in = args.tables_in.lower()
    # 配置表文件判断
    # if args.tabfile_flag is None:
    #     tabfile_flag = 'N'
    # else:
    #     if args.tabfile_flag.upper() == 'N' or args.tabfile_flag.upper() == 'Y':
    #         tabfile_flag = args.tabfile_flag
    #     else:
    #         mdtool.log.error("tabfile_flag是否读取文件参数配置错误，请检查")
    #         sys.exit()
    # if tabfile_flag.upper() == 'Y':
    #     if args.tabfile is None:
    #         mdtool.log.error("tabfile同步的表配置文件名称参数配置错误，请检查")
    #         sys.exit()
    #     else:
    #         tabfile = args.tabfile
    #         array = []
    #         with open(mdtool.Variable.CONF_PATH + os.sep + tabfile, 'r', encoding='utf-8') as f:
    #             lines = f.readlines()
    #             for line in lines:
    #                 array.append(line.strip('\n').lower())
    #         tables_in = ','.join(array)
    #         f.close()
    # 是否加载到DB
    if args.dbin is None:
        dbin = 'N'
    else:
        if args.dbin.upper() == 'N' or args.dbin.upper() == 'Y':
            dbin = args.dbin
        else:
            mdtool.log.error("dbin是否直接加载到DB参数配置错误，请检查")
            sys.exit()
    # 导出对象信息
    mls = modelgenerator.ModelDialect(dbsrc, dbmgr, flag, tables_in)
    mls.main()

    # 对象生成器
    mlm = modelgenerator.Modelgenerator(dbsrc, dbmgr, dbtag, tables_in)
    # 表对象
    mlm.tablesGenerator()
    # 约束对象
    mlm.constraintsGenerator()
    # 索引对象
    mlm.indexesGenerator()

    if dbin == 'Y':
        # 模型加载到目标库
        # 添加唯一性索引后可以再添加主键，故先执行索引对象
        mlt = modelgenerator.Modeltodb(dbsrc, dbtag, flag)
        mlt.modelToDB()


if __name__ == '__main__':
    main()

# --dbsrc ORACLE_10.45.59.246 --dbtag MYSQL_172.21.86.205 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL Done
# --dbsrc ORACLE_10.45.59.246 --dbtag POSTGRESQL_172.21.86.201 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL

# --dbsrc MYSQL_172.21.86.205 --dbtag ORACLE_172.21.86.201 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL Done
# --dbsrc MYSQL_172.21.86.205 --dbtag POSTGRESQL_172.21.86.201 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL

# --dbsrc POSTGRESQL_172.21.86.201 --dbtag ORACLE_172.21.86.201 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL Done
# --dbsrc POSTGRESQL_172.21.86.201 --dbtag MYSQL_172.21.86.205 --dbmgr MGR_172.21.86.205  --tables_in BFM_AREA,BFM_BULLETIN_LEVEL

# --dbsrc POSTGRESQL_172.21.86.201 --dbtag MYSQL_172.21.86.205 --dbmgr MGR_172.21.86.205 --dbin yyyy --tabfile_flag tttttt
