# -*- coding: utf-8 -*-
# @Time    : 2020/12/7 10:15
# @Author  : Fcvane
# @Param   : 
# @File    : gui_2.py
import tkinter as tk
import socket
import threading

root = tk.Tk()
root.title("hello world")
root.geometry('600x400')
root.resizable(width=False, height=True)

# s = socket.socket()
# s.connect(('192.168.8.7', 6969))

# tk.Label(root, text='状态栏', font=('Arial', 20), width=200, height=1, bg='pink').pack()

frm_top = tk.Frame(root)
tk.Label(frm_top,
         text='状态栏1',
         font=('Arial', 20),
         bg='yellow',
         width=40).pack(side=tk.TOP)
frm_top.pack(side=tk.TOP)

frm_mid = tk.Frame(root)
frm_mid_left = tk.Frame(frm_mid)
tk.Label(frm_mid_left,
         text='聊天窗口',
         font=('Arial', 20),
         bg='pink',
         height=8, width=30).pack(side=tk.TOP)
frm_mid_left.pack(side=tk.LEFT)
frm_mid_right = tk.Frame(frm_mid)
tk.Label(frm_mid_right,
         text='聊天列表',
         font=('Arial', 20),
         bg='blue',
         height=8, width=10).pack(side=tk.TOP)
frm_mid_right.pack(side=tk.RIGHT)
frm_mid.pack(side=tk.TOP)

frm_bot = tk.Frame(root)
frm_bot_left = tk.Frame(frm_bot)
tk.Label(frm_bot_left,
         text='消息发送框',
         font=('Arial', 20),
         bg='green',
         height=3, width=30).pack(side=tk.TOP)
frm_bot_left.pack(side=tk.LEFT)
frm_bot_right = tk.Frame(frm_bot)
tk.Label(frm_bot_right,
         text='按钮',
         font=('Arial', 20),
         bg='red',
         height=3, width=10).pack(side=tk.TOP)
frm_bot_right.pack(side=tk.RIGHT)
frm_bot.pack(side=tk.TOP)

frm_bot_but = tk.Frame(frm_mid_right)

root.mainloop()
