#!/bin/bash
#Usage check datax log
#filename:checklog.sh
if [ $# == 1 ]
then
  day=$1
else
  day=`date +"%Y-%m-%d"`
fi

tabdir=$DATAX_HOME/log/${day}

n=`find ${tabdir} -name "*.log" | xargs grep "ERROR" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | sort -u | wc -l`
m=`ls ${tabdir}/*.log | wc -l`
echo "[ CheckDatax ]Job number :$m"
echo "----------------------------------------------------------------------------ERROR"
echo "[ CheckDatax ]Failure job number :$n"
echo "[ CheckDatax ]Failure job datax detail information:"
echo "[ CheckDatax ]DataX XML directory /home/script/job"
find ${tabdir} -name "*.log" | xargs grep "ERROR" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | awk  -F '_xml' '{print $1".xml"}' | sort -u
echo ""
echo "[ CheckDatax ]Check directory in /home/script/datax/log/${day}"
find ${tabdir} -name "*.log" | xargs grep "ERROR" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | sort -u
echo ""
echo "[ CheckDatax ]Failure job tablename detail information:"
find ${tabdir} -name "*.log" | xargs grep "ERROR" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | awk  -F '_xml' '{print $1}' | awk -F 'OraToPG_' '{print $2}' | sort -u
echo "----------------------------------------------------------------------------ROLLBACK"
find ${tabdir} -name "*.log" | xargs grep "回滚此次写入" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | sort -u
s=`expr $m - $n`
echo "----------------------------------------------------------------------------SUCCESS"
echo "[ CheckDatax ]Success job number :$s"

#补录
#find ${tabdir} -name "*.log" | xargs grep "ERROR" | awk -F ':' '{print $1}' | sort -u | awk -F '-' '{print $3}' | awk -F '/' '{print $2}' | awk  -F '_xml' '{print $1}' | awk -F 'OraToPG_' '{print $2}' | sort -u > /home/fcvane/script/conf/bl.lst