# -*- coding: utf-8 -*-
# @Time    : 2020/9/21 13:26
# @Author  : Fcvane
# @Param   : 
# @File    : tmp_2.py
from multiprocessing.dummy import Pool
import time
import os

def talk(msg):
    print('msg:',msg)
    time.sleep(3)
    print('end')
    print("子进程ID %s, 主进程ID %s" % (os.getpid(),os.getppid()))

if __name__ == "__main__":
    print('开始执行程序：')
    start_time = time.time()
    pool = Pool(3)
    print('开始执行三个子进程')
    for i in range(6):
        # pool.apply(talk,[i])
        pool.apply_async(talk,[i])
    pool.close()
    pool.join()
    print("主进程ID %s " % os.getpid())
