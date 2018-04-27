# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:37:52 2018

@author: hadoop
"""

# 对系统超参数进行读取，返回超参数字典
def SuperParasReader():
    superParasDict = {}
    with open("SuperParas.txt",'r') as superParasFile:
        for item in superParasFile.readlines():
            item = item.strip()
            if item.startswith('#') or item == "":
                continue
            
            key,value,Type = item.split('=')
            if Type != 'str':
                exec("value=%s(%s)"%(Type,value))
            superParasDict[key] = value
    return superParasDict
    
if __name__ == '__main__':
    superParasDict = SuperParasReader()