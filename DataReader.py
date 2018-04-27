# -*- coding: utf-8 -*-
"""
Created on Fri Apr 13 17:18:29 2018

@author: hadoop
"""

import pandas as pd
from abc import ABCMeta,abstractmethod
from MySQLConnector import MySQLConnector
import re
import numpy as np
from tools import tools_DateTimeTrans
from SuperParasReader import SuperParasReader

SuperParasDict = SuperParasReader()

class Abs_DataReader(object):
    __metaclass__ = ABCMeta
    def __init__(self,startDate,endDate,substationID,code):
        '''
            startDate:str,%Y-%m-%d %H:%M:%S
            endDate:str,%Y-%m-%d %H:%M:%S
        '''
        self.startDate = startDate
        self.endDate = endDate
        self.substationID = substationID
        self.code = code
        self._SQLState()
        self.dateIndex = pd.date_range(startDate,endDate,freq='1min')
        
    
    @abstractmethod
    def _SQLState(self):
        '''
            不同的项目有不同的查询语句，所以该方法需要被子类重载以生成合适的SQL语句。该方法应该生成三条SQL语句，分别是
            _QueryBoilerState:查询目标时间段的锅炉相关数据
            _QueryWeatherState:查询目标时间段的室外气象站相关数据
            _QueryInHomeState:查询目标时间段的室内温湿度传感器数据
            _QueryHRZState:查询目标时间段的换热站相关数据

            参数：
            startDate:"%Y-%m-%d %H:%M:%D"
                查询数据的开始时间
            endDate:"%Y-%m-%d %H:%M:%D"
                查询数据的结束时间
    
            提供的全局变量：
            self._QueryBoilerState:查询目标时间段的锅炉相关数据
            self._QueryWeatherState:查询目标时间段的室外气象站相关数据
            self._QueryInHomeState:查询目标时间段的室内温湿度传感器数据
            self._QueryHRZState:查询目标时间段的换热站相关数据
            
        '''
    @abstractmethod
    def _BoilerColumnsMap(self):
        '''
            子类应该重载该方法以解决数据库中字段名不规范的问题

            参数：
                无
    
            提供的全局变量：
                无
    
            返回值：
                字段名映射字典，如果不需要调整，可以返回一个空字典，如果某一个字段需要修正，则需要返回全字段的映射字典
        '''
        
    @abstractmethod
    def _WeatherColumnsMap(self):
        '''
            子类应该重载该方法以解决室外气象站数据库中字段名不规范的问题

            参数：
                无
    
            提供的全局变量：
                无
    
            返回值：
                字段名映射字典，如果不需要调整，可以返回一个空字典，如果某一个字段需要修正，则需要返回全字段的映射字典
        '''
        
    @abstractmethod
    def _InHomeColumnsMap(self):
        '''
            子类应该重载该方法以解决室内温湿度数据库中字段名不规范的问题

            参数：
                无
    
            提供的全局变量：
                无
    
            返回值：
                字段名映射字典，如果不需要调整，可以返回一个空字典，如果某一个字段需要修正，则需要返回全字段的映射字典
        '''
        
    @abstractmethod
    def _CollectInHomeDeviceID(self):
        """
            子类应该重载该方法以返回所有的室内传感器的设备ID

            参数：
                无
    
            提供的全局变量：
                无
    
            返回值：
                该方法应该返回一个List，该List包含全部的室内传感器设备ID
        """
    @abstractmethod
    def _unnormalDataPrecess(self,a,b,c,d):
        """
            子类应该重载该方法以提供一种对缺失值和异常值进行处理的机制
            
            参数：DataFrame
                a: 对应锅炉数据
                b: 对应天气数据
                c: 对应室内传感器数据
            
            提供的全局变量：
                无
                
            返回值：DataFrame
                对异常值和缺失值进行修正之后的数据集
        """
    @abstractmethod
    def _HRZColumnsMap(self):
         '''
            子类应该重载该方法以解决室内温湿度数据库中字段名不规范的问题

            参数：
                无
    
            提供的全局变量：
                无
    
            返回值：
                字段名映射字典，如果不需要调整，可以返回一个空字典，如果某一个字段需要修正，则需要返回全字段的映射字典
        '''
    
    def concatData(self,a,b,c,d):
        """
            a: 对应锅炉数据
            b: 对应天气数据
            c: 对应室内传感器数据
            d: 对应换热站数据
        """
        a,b,c,d = self._unnormalDataPrecess(a,b,c,d)
        # 做mean操作的时候不太存在掩盖缺失值的风险，做interpolate操作时有较大的掩盖缺失值的风险
        totalData = pd.DataFrame(index=self.dateIndex)
        
        if SuperParasDict['BoilerFreq'] > 60:
            method = 'interpolate'
        else:
            method = 'mean'
        exec("modified_A = a.resample('1min').%s()"%method)
        for item in modified_A:
            totalData[item] = modified_A[item]
        if SuperParasDict['WeatherFreq'] > 60:
            method = 'interpolate'
        else:
            method = 'mean'
        exec("modified_B = b.resample('1min').%s()"%method)
        for item in modified_B:
            totalData[item] = modified_B[item]
        if SuperParasDict['InHomeFreq'] > 60:
            method = 'interpolate'
        else:
            method = 'mean'
        for item in c:
            exec("modified_Item = item.resample('1min').%s()"%method)
            for i in modified_Item:
                totalData[i] = modified_Item[i]
        if len(d) > 0:
            if SuperParasDict['HRZFreq'] > 60:
                method = 'interpolate'
            else:
                method = 'mean'
            exec("modified_D = d.resample('1min').%s()"%method)
            for item in modified_D:
                totalData[item] = modified_D[item]
        return totalData
            
        
    def readData(self):
        # 建立对数据库的连接
        conn = MySQLConnector()
        conn.openConnector()
        # ++++++++++++++ 查询锅炉状态 +++++++++++++++++++++++++++++++++++
        # 从SQL语句中分析所需要抓取的字段名模式
        columnsPattern = re.compile("SELECT DISTINCT (.*?) FROM")
        columnsStr = re.findall(columnsPattern,self._QueryBoilerState)[0]
        columns = columnsStr.strip().split(',')
        columns = [(c.strip())[2:] for c in columns]
        # 对字段名进行修正
        if len(self._BoilerColumnsMap()) != 0:
            columns = [self._BoilerColumnsMap()[c] for c in columns]
        # 游标执行查询锅炉状态数据的语句
        conn.cursor.execute(self._QueryBoilerState)
        tempBoilerState = np.array(conn.cursor.fetchall())
        #     对该数据集中日期时间进行修正
        modifiedTime = [tools_DateTimeTrans(i) for i in tempBoilerState[:,0]]
        BoilerData = pd.DataFrame(np.array(tempBoilerState[:,1:],np.float32),index=modifiedTime,
                                  columns=columns[1:])
        # ++++++++++++++ 查询锅炉状态 +++++++++++++++++++++++++++++++++++
        # ++++++++++++++ 查询天气状态 +++++++++++++++++++++++++++++++++++
        # 从SQL语句中分析所需要抓取的字段名模式
        columnsStr = re.findall(columnsPattern,self._QueryWeatherState)[0]
        columns = columnsStr.strip().split(',')
        columns = [c.strip()[2:] for c in columns]
        # 对字段名进行修正
        if len(self._WeatherColumnsMap()) != 0:
            columns = [self._WeatherColumnsMap()[c] for c in columns]
        # 游标执行查询天气状态数据的语句
        conn.cursor.execute(self._QueryWeatherState)
        tempWeatherState = np.array(conn.cursor.fetchall())
        modifiedTime = [tools_DateTimeTrans(i) for i in tempWeatherState[:,0]]
        WeatherData = pd.DataFrame(np.array(tempWeatherState[:,1:],np.float32),index = modifiedTime,
                                   columns=columns[1:])
        # ++++++++++++++ 查询天气状态 +++++++++++++++++++++++++++++++++++
        # ++++++++++++++ 查询室内状态 +++++++++++++++++++++++++++++++++++
        dev_ids = self._CollectInHomeDeviceID()
        dev_DataFrames = []
        for k,dev_id in enumerate(dev_ids):
            # 生成每一个设备对应的SQL语句
            individualDevSQL = self._QueryInHomeState%(dev_id)
            # 从SQL语句中抽取所抽取的字段
            columnsStr = re.findall(columnsPattern,individualDevSQL)[0]
            # 对字段名称进行修正
            columns = columnsStr.strip().split(',')
            columns = [c.strip()[2:] for c in columns]
            if len(self._InHomeColumnsMap()) != 0:
                columns = [self._InHomeColumnsMap()[c] for c in columns]
            columns = [u'%d#传感器%s'%(k,c) for c in columns]
            # 游标执行查询室内状态数据的语句
            conn.cursor.execute(individualDevSQL)
            tempInHomeState = np.array(conn.cursor.fetchall())
            try:
                modeifiedTime = [tools_DateTimeTrans(i) for i in tempInHomeState[:,0]]
                InHomeData = pd.DataFrame(np.array(tempInHomeState[:,1:],np.float32),index = modeifiedTime,
                                      columns=columns[1:])
                dev_DataFrames.append(InHomeData)
            except IndexError:
                print u'%d#室内传感器无数据'%k
                continue
        # ++++++++++++++ 查询室内状态 +++++++++++++++++++++++++++++++++++
        # ++++++++++++++ 查询换热站状态 +++++++++++++++++++++++++++++++++
        if self._QueryHRZState != False:
            # 从SQL语句中分析所需要抓取的字段名模式
            columnsStr = re.findall(columnsPattern,self._QueryHRZState)[0]
            columns = columnsStr.strip().split(',')
            columns = [c.strip()[2:] for c in columns]
            # 对字段名进行修正
            if len(self._HRZColumnsMap()) != 0:
                columns = [self._HRZColumnsMap()[c] for c in columns]
            # 游标执行查询天气状态数据的语句
            conn.cursor.execute(self._QueryHRZState)
            tempHRZState = np.array(conn.cursor.fetchall())
            modifiedTime = [tools_DateTimeTrans(i) for i in tempHRZState[:,0]]
            HRZData = pd.DataFrame(np.array(tempHRZState[:,1:],np.float32),index = modifiedTime,
                                   columns=columns[1:])
        else:
            HRZData = pd.DataFrame()
        totalData = self.concatData(BoilerData,WeatherData,dev_DataFrames,HRZData)
        return totalData

# 样例测试     
class __XK(Abs_DataReader):
    def _SQLState(self):
        self._QueryBoilerState = u"SELECT DISTINCT a.create_time,a.节能器出水温度1,\
        a.炉膛压力1,a.出水温度1,a.室外温度1,a.炉膛温度1,a.目标温度设定1,a.回水温度1,a.鼓风机电流1,\
        a.回水压力1,a.出水压力1,a.节能器出水压力1,a.排烟温度1,a.节能器进口烟温1,a.鼓风机频率1,a.节能器进水温度1,a.FI001A,b.瞬时流量,b.温度 FROM bd_xinkou_1 a LEFT JOIN bd_xinkou_2 b on a.create_time = b.create_time WHERE a.create_time >= '%s' AND a.create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryWeatherState = "SELECT DISTINCT t.create_time,t.temp,t.hr,t.lux,t.wind_speed,t.wind_direction \
          FROM bd_weather_station t WHERE dev_id=40002704 and create_time >= '%s' and create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryInHomeState = "SELECT DISTINCT t.create_time,t.temp,t.hr FROM bd_temp_hr t WHERE dev_id='%s' AND" + " create_time >= '%s' AND create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryHRZState = u"SELECT DISTINCT t.create_time,t.一次供温度,t.一次回温度,t.二次供温度,t.二次回温度 FROM bd_xinkou_hrz t WHERE project_sub_station_id=%d and code='%s' and t.create_time>='%s' and t.create_time<'%s'"%(self.substationID,self.code,self.startDate,self.endDate)
    
    def _BoilerColumnsMap(self):
        return {'create_time':'create_time',u'节能器出水温度1':u'节能器出水温度',
                u'炉膛压力1':u"炉膛压力",u'出水温度1':u'出水温度',u'室外温度1':u"室外温度",
                u'炉膛温度1':u'炉膛温度',u'目标温度设定1':u"目标温度设定",u'回水温度1':u'回水温度',
                u'鼓风机电流1':u"鼓风机电流",u"回水压力1":u"回水压力",u"出水压力1":u'出水压力',
                u'节能器出水压力1':u"节能器出水压力",u'排烟温度1':u"排烟温度",u'节能器进口烟温1':u"节能器进口烟温",
                u"鼓风机频率1":u"鼓风机频率",u'节能器进水温度1':u"节能器进水温度",u'瞬时流量':u"瞬时流量",
                u"温度":u"燃气温度",u'FI001A':u'供水流量'}
    
    def _WeatherColumnsMap(self):
        return {"temp":u"气象站室外温度",'hr':u'气象站室外湿度','lux':u"气象站室外光照",
                'wind_speed':u"气象站室外风速",'wind_direction':u'气象站室外风向',
                'create_time':'create_time'}
    
    def _InHomeColumnsMap(self):
        return {"temp":u"室内温度",'hr':u"室内湿度",'create_time':'create_time'}
        
    def _HRZColumnsMap(self):
        return {}
    
    def _CollectInHomeDeviceID(self):
        return ["W634iMCwmSCcjQkltb7d38btv000%02d"%i for i in [18,6,23,1,13,12,4,5,24,14]]
    
    def _unnormalDataPrecess(self,a,b,c,d):
        return a,b,c,d

class __NF(Abs_DataReader):
    def _SQLState(self):
        self._QueryBoilerState = u"SELECT DISTINCT t.create_time,\
          t.一号燃气流量,t.二号燃气流量,t.三号燃气流量,t.一号锅炉出水压力,t.一号锅炉供水流量,\
          t.二号锅炉出水压力,t.二号锅炉供水流量,t.三号锅炉出水压力,t.三号锅炉供水流量,\
          t.一次总管供水温度,t.一次总管回水温度 FROM bd_ningfa_1 t WHERE create_time > '%s' and create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryWeatherState = "SELECT DISTINCT t.create_time,t.temp,t.hr,t.lux,t.wind_speed,t.wind_direction \
          FROM bd_weather_station t WHERE dev_id=40002712 and create_time > '%s' and create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryInHomeState = "SELECT DISTINCT t.create_time,t.temp,t.hr FROM bd_temp_hr t WHERE dev_id='%s' AND"+" create_time > '%s' AND create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryHRZState = False
    def _BoilerColumnsMap(self):
        return {}
    
    def _WeatherColumnsMap(self):
        return {"temp":u"气象站室外温度",'hr':u'气象站室外湿度','lux':u"气象站室外光照",
                'wind_speed':u"气象站室外风速",'wind_direction':u'气象站室外风向',
                'create_time':'create_time'}
        
    def _InHomeColumnsMap(self):
        return {"temp":u"室内温度",'hr':u"室内湿度",'create_time':'create_time'}
    
    def _HRZColumnsMap(self):
        return {}
    
    def _unnormalDataPrecess(self,a,b,c,d):
        # 对气象数据中的光照进行修正
        temp = []
        for item in b[u'气象站室外光照'].as_matrix():
            if item < -50:
                temp.append(31000)
            elif item < 0:
                temp.append(0)
            else:
                temp.append(item)
        b[u'气象站室外光照'] = temp
        
        return a,b,c,d
    
    def _CollectInHomeDeviceID(self):
        return ["W634iMCwmSCcjQkltb7d38btv000%02d"%i for i in [30,8,9,28,26,10,16,3,38,52,47,60]]
    
        
if __name__ == '__main__':
    #a = __XK('2018-01-10 00:00:00',"2018-02-26 10:00:00",24,'N1')
    #b = a.readData()
    c = __NF('2018-01-10 00:00:00',"2018-02-26 10:00:00",0,'0')
    d = c.readData()
