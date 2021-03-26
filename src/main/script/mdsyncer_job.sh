#!/bin/bash
#Usage：
#	调用mdsyncer
#	因为外键存在对于数据同步存在影响，故不直接使用mdsyncer工具将模型写入数据库，通过shell来按类别是实现。
#	Shell需要oracle和pg客户端，该部分内容集成在tar包中
#	1.配置源数据库、目标数据库以及需要同步的表信息
#	2.执行模型同步
#	3.通过shell将模型对象SQL写入目标数据库，备注：外键暂不处理，等待数据同步完成后才处理
#	4.数据同步+同步结果校验
#	5.执行外键创建SQL，完成
#filename:mdsyncer_job.sh
#eg:sh mdsyncer_job.sh ORACLE_172.21.86.201 POSTGRESQL_172.21.86.201 MGR_172.21.86.205 sample_tab.lst

dbsrc=$1
dbtag=$2
dbmgr=$3
tabfile=$4

tabdir=/home/fcvane/mdsyncer/src/main

#oracle数据库客户端目录
oradir=${tabdir}/instantclient_12_2
#postgresql客户端目录
#pgdir=${tabdir}/postgresql
pgdir=${tabdir}/postgresql
#mdsyncer工具目录
mdsyncer_dir=${tabdir}/mdsyncer
#加载环境变量
export ORACLE_HOME=${oradir}
export LD_LIBRARY_PATH=${oradir}:${pgdir}/lib
export TNS_ADMIN=${oradir}
export PATH=$PATH:${oradir}
export PG_HOME=${pgdir}
export PATH=$PATH:${PG_HOME}/bin


#对象SQL目录
table_dir=${mdsyncer_dir}/result/tables_generator
constraint_dir=${mdsyncer_dir}/result/constraints_generator
index_dir=${mdsyncer_dir}/result/indexes_generator

#配置文件信息
cfg_dir=${mdsyncer_dir}/conf/dbParams

#目标库数据库信息获取
PGHOST=`cat ${cfg_dir}/dbconf.xml | grep -A 6 "$dbtag" | grep "host" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
PGPORT=`cat ${cfg_dir}/dbconf.xml | grep -A 6 "$dbtag" | grep "port" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
PGSCHEMA=`cat ${cfg_dir}/dbconf.xml | grep -A 6 "$dbtag" | grep "dbname" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
PGUSER=`cat ${cfg_dir}/dbconf.xml | grep -A 6 "$dbtag" | grep "user" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
PGPASSWD_DP=`cat ${cfg_dir}/dbconf.xml | grep -A 6 "$dbtag" | grep "passwd" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
PGPASSWD=`cd ${mdsyncer_dir}/bin && ./mdtool -dp ${PGPASSWD_DP}`
PGPSQL="${PGUSER}:${PGPASSWD}@${PGHOST}:${PGPORT}/${PGSCHEMA}"

#模型同步
cd ${mdsyncer_dir}/bin && ./modelsyncer_tabdb --dbsrc $dbsrc --dbtag $dbtag --dbmgr $dbmgr --dbin N --tabfile_flag Y --tabfile $tabfile

#模型同步完成后执行表、约束(剔除外键)、索引等对象创建SQL
#表对象
${pgdir}/bin/psql postgresql://${PGPSQL} -f ${table_dir}/oracle2postgresql/oracle2postgresql_model.sql > /dev/null 2>$1
#索引对象
${pgdir}/bin/psql postgresql://${PGPSQL} -f ${index_dir}/oracle2postgresql/oracle2postgresql_index_model.sql > /dev/null 2>$1
#约束对象
${pgdir}/bin/psql postgresql://${PGPSQL} -f ${constraint_dir}/oracle2postgresql/oracle2postgresql_constraint_model.sql > /dev/null 2>$1

#数据同步
cd ${mdsyncer_dir}/bin && ./datasyncer --dbsrc $dbsrc --dbtag $dbtag  --tabfile $tabfile --dbin Y

day=`date +"%Y-%m-%d"`

#同步结果校验
${tabdir}/script/checklog.sh $day

#执行外键模型创建
${pgdir}/bin/psql postgresql://${PGPSQL} -f ${constraint_dir}/oracle2postgresql/oracle2postgresql_constraint_fk_model.sql


