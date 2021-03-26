# -*- coding: utf-8 -*-
# @Time    : 2020/9/10 20:07
# @Author  : Fcvane
# @Param   : 
# @File    : datasyncer.py

import profilegenerator
import mdtool
import argparse
import os, sys



def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--dbsrc', dest='dbsrc', required=True, help='源数据库信息')
    parser.add_argument('--dbtag', dest='dbtag', required=True, help='目标数据库信息')
    parser.add_argument('--tabfile', dest='tabfile', required=True, help='需要同步的表配置文件名称')
    parser.add_argument('--dbin', dest='dbin', required=False,
                        help='是否直接加载到DB: Y生成datax配置文件且加载到目标DB;N只生成datax配置文件；默认不直接加载')

    args = parser.parse_args()
    dbsrc = mdtool.xmler(args.dbsrc).dbCFGInfo()
    dbtag = mdtool.xmler(args.dbtag).dbCFGInfo()
    tabfile = args.tabfile

    tables_in = []
    with open(mdtool.Variable.CONF_PATH + os.sep + tabfile, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            tables_in.append(line.strip('\n').lower())

    # Json任务生成器
    pfs = profilegenerator.ProfileGenerator(dbsrc, dbtag, tables_in)
    pfs.Profilejson()

    # 是否加载到DB
    if args.dbin is None:
        dbin = 'N'
    else:
        if args.dbin.upper() == 'N' or args.dbin.upper() == 'Y':
            dbin = args.dbin
        else:
            mdtool.log.error("dbin是否直接加载到DB参数配置错误，请检查")
            sys.exit()

    if dbin == 'Y':
        # 添加多线程
        from multiprocessing import Pool
        # 执行
        pool = Pool(processes=6)
        JOB_PATH = mdtool.Variable.JOB_PATH
        for root, dirs, files in os.walk(JOB_PATH):
            # 遍历文件
            for f in files:
                file = os.path.join(root, f)
                # pfs.DataxExecute(file)
                pool.apply_async(func=pfs.DataxExecute, args=(file,))
        pool.close()
        pool.join()


if __name__ == '__main__':
    main()

# --dbsrc ORACLE_10.45.59.246 --dbtag POSTGRESQL_172.21.86.201 --tabfile sample_tab.lst
