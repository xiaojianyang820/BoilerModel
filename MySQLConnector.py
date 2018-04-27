# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:18:29 2018

@author: hadoop
"""

import MySQLdb
import time
from SuperParasReader import SuperParasReader

SuperParasDict = SuperParasReader()
Host = SuperParasDict['Host']
Port = SuperParasDict['Port']
User = SuperParasDict['User']
Passwd = SuperParasDict['Passwd']
Db = SuperParasDict['Db']
Charset = SuperParasDict['Charset']

class MySQLConnector(object):
    def openConnector(self):
        '''
            Try to connect to the MySQL Server until succeed
        '''
        # 记录是否链接数据库成功
        self.__connected = False
        while not self.__connected:
            try:
                self.__connToDataSource = MySQLdb.connect(host=Host,port=Port,
                                user=User,passwd=Passwd,db=Db,charset=Charset)
                self.__connected = True
            except:
                print u'尝试重新连接数据库中 ...'
                time.sleep(1)
        self.cursor = self.__connToDataSource.cursor()
        
    def closeConnector(self):
        if self.__connected:
            self.cursor.close()
            self.__connToDataSource.close()
        else:
            print u'尚未对数据库的连接，所以不能关闭连接'
            
if __name__=='__main__':
    conn = MySQLConnector()
    conn.openConnector()
    conn.cursor.execute("show tables")
    tables = conn.cursor.fetchall()
    print tables