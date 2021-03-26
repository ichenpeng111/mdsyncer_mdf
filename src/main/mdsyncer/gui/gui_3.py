# -*- coding: utf-8 -*-
# @Time    : 2020/12/7 10:42
# @Author  : Fcvane
# @Param   : 
# @File    : gui_3.py

from tkinter import *
import time

t = Tk()
t.title('与python聊天中')

#创建frame容器
frmLT = Frame(width=500, height=320, bg='white')
frmLC = Frame(width=500, height=150, bg='white')
frmLB = Frame(width=500, height=30)
frmRT = Frame(width=200, height=500)

frmLT.grid(row=0, column=0,padx=1,pady=3)
frmLC.grid(row=1, column=0,padx=1,pady=3)
frmLB.grid(row=2, column=0)
frmRT.grid(row=0, column=1, rowspan=3,padx=2,pady=3)

'''#固定容器大小
frmLT.grid_propagate(0)
frmLC.grid_propagate(0)
frmLB.grid_propagate(0)
frmRT.grid_propagate(0)'''

#添加按钮
btnSend = Button(frmLB, text='发 送', width = 8)#在frmLB容器中添加
btnSend.grid(row=2,column=0)
btnCancel = Button(frmLB, text='取消', width = 8)
btnCancel.grid(row=2,column=1,sticky=E)

#添加图片
imgInfo = PhotoImage(file = "")
lblImage = Label(frmRT, image = imgInfo)
lblImage.image = imgInfo
lblImage.grid()

#固定容器大小
frmLT.grid_propagate(0)
frmLC.grid_propagate(0)
frmLB.grid_propagate(0)
frmRT.grid_propagate(0)

t.mainloop()