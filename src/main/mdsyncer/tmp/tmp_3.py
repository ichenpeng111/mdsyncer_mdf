# -*- coding: utf-8 -*-
# @Time    : 2020/10/12 14:06
# @Author  : Fcvane
# @Param   : PersistentDB - 每个线程创立一个连接（不推荐）
# @File    : tmp_3.py
'''
    POOL = PersistentDB()  - 实例化池对象，写入池的配置信息；为每个线程创立连接
    conn = POOL.connection() - 池连接
    conn.cursor().execute('select * from user') - 执行SQL语句
    conn.cursor().fetchall() - 获取执行SQL后的返回
    conn.close() - 未关闭连接，只是将连接放回池子，仅供自己的线程再次使用；当线程终止时，连接自动关闭；
'''
# 单线程操作
from dbutils.persistent_db import PersistentDB
import pymysql
'''
creator=psycopg2,  # 使用链接数据库的模块mincached
maxconnections=6,  # 连接池允许的最大连接数，0和None表示不限制连接数
mincached=1,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
maxcached=4,  # 链接池中最多闲置的链接，0和None不限制
blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
setsession=[],  # 开始会话前执行的命令列表。
'''
POOL = PersistentDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    closeable=False,
    # 如果为False时， conn.close() 实际上被忽略，供下次使用，再线程关闭时，才会自动关闭链接。如果为True时， conn.close()则关闭链接，那么再次调用pool.connection时就会报错，因为已经真的关闭了连接（pool.steady_connection()可以获取一个新的链接）
    threadlocal=None,  # 本线程独享值得对象，用于保存链接对象，如果链接对象被重置
    host='172.21.86.205',
    port=3306,
    user='root',
    password='abc123',
    database='test',
    charset='utf8'
)


def func():
    conn = POOL.connection(shareable=False)
    cursor = conn.cursor()
    cursor.execute('select * from fcvane')
    result = cursor.fetchall()
    print(result)
    cursor.close()
    conn.close()


if __name__ == '__main__':
    func()
