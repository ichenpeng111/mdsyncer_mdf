# -*- coding: utf-8 -*-
# @Time    : 2020/12/7 9:42
# @Author  : Fcvane
# @Param   : 
# @File    : mdv.py

import pymysql
from tkinter import *
from tkinter import ttk

def query_db():
    # 连接sqlite3 数据库
    database = conn = pymysql.connect(host='172.21.86.205', port=3306,
                                      user='root', passwd='abc123', db='mdsyncer',
                                      charset='utf8')
    cursor = database.cursor()
    # 用执行select语句查询数据
    sql = "select * from cfg_tables limit 10"
    cursor.execute(sql)
    # 通过游标的description属性获取列信息
    description = cursor.description
    # 使用fetchall获取游标中的所有结果集
    rows = cursor.fetchall()
    cursor.close()
    database.close()
    return description, rows


def main():
    root = Tk()
    # 标题
    root.title('Model&Data Synchronization Tool ( MDSyncer )')
    # 窗口大小
    root.geometry('600x400')
    # root.resizable(width=False, height=False)
    # 在大窗口下定义一个顶级菜单实例
    menubar = Menu(root)

    # 在顶级菜单实例下创建子菜单实例
    fmenu = Menu(menubar)
    for each in ['新建', '打开', '保存', '另存为']:
        fmenu.add_command(label=each)

    emenu = Menu(menubar)
    # 为每个子菜单实例添加菜单项
    for each in ['复制', '粘贴', '剪切']:
        emenu.add_command(label=each)

    tmenu = Menu(menubar)
    for each in ['默认视图', '新式视图']:
        tmenu.add_command(label=each)

    hmenu = Menu(menubar)
    for each in ['版权信息', '联系我们']:
        hmenu.add_command(label=each)

    # 为顶级菜单实例添加菜单，并级联相应的子菜单实例
    menubar.add_cascade(label='文件(F)', menu=fmenu)
    menubar.add_cascade(label='编辑(E)', menu=emenu)
    menubar.add_cascade(label='工具(T)', menu=tmenu)
    menubar.add_cascade(label='帮助(H)', menu=hmenu)
    # 创建frame容器
    frm = Frame(width=600, height=400, bg='white')
    frm.grid(row=0, column=0, padx=0, pady=0)

    # 菜单实例应用到大窗口中
    root.configure(menubar)
    root.mainloop()


if __name__ == '__main__':
    main()
