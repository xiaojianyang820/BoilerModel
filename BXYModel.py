# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 14:51:06 2018

@author: hadoop
"""

from DataReader import Abs_DataReader
from tools import tools_DateTimeTrans,Timer,StandTimer
import numpy as np
from CNNModel import CNNModel
from SuperParasReader import SuperParasReader
import matplotlib.pyplot as plt
from WeatherReporter import weatherPredictor
from scipy.optimize import curve_fit
from OutterChecker import OutterChecker
from MainModel import MainModel



class XK(Abs_DataReader):
    def _SQLState(self):
        self._QueryBoilerState = u"SELECT DISTINCT a.create_time,a.节能器出水温度1,\
        a.炉膛压力1,a.出水温度1,a.室外温度1,a.炉膛温度1,a.目标温度设定1,a.回水温度1,a.鼓风机电流1,\
        a.回水压力1,a.出水压力1,a.节能器出水压力1,a.排烟温度1,a.节能器进口烟温1,a.鼓风机频率1,a.节能器进水温度1,a.FI001A,b.瞬时流量,b.温度 FROM bd_xinkou_1 a LEFT JOIN bd_xinkou_2 b on a.create_time = b.create_time WHERE a.create_time > '%s' AND a.create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryWeatherState = "SELECT DISTINCT t.create_time,t.temp,t.hr,t.lux,t.wind_speed,t.wind_direction \
          FROM bd_weather_station t WHERE dev_id=40002704 and create_time > '%s' and create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryInHomeState = "SELECT DISTINCT t.create_time,t.temp,t.hr FROM bd_temp_hr t WHERE dev_id='%s' AND" + " create_time > '%s' AND create_time < '%s'"%(self.startDate,self.endDate)
        self._QueryHRZState = u"SELECT DISTINCT t.create_time,t.一次供温度,t.一次回温度,t.二次供温度,t.二次回温度 FROM bd_xinkou_hrz t WHERE project_sub_station_id=%d and code='%s' and t.create_time>'%s' and t.create_time<'%s'"%(self.substationID,self.code,self.startDate,self.endDate)
    
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
        
class BXYMain(MainModel):
    def dataReader(self,startDate,endDate,HRZID,Code):
        return XK(startDate,endDate,HRZID,Code)
    
    def genePsudoWeather(self,stageData,weatherIndex,pLength):
        return [-1]
        

if __name__ == '__main__':
    # 声明当前使用的时钟对象
    Clock = Timer('2018-02-26 00:00:00',15)
    # 声明当前使用的模型对象
    Model = CNNModel
    BXY_1 = BXYMain(True,'2017-12-15 00:00:00',Clock,30,Model,
                    [u"瞬时流量",u'气象站室外温度',u"气象站室外湿度",u"气象站室外风速",
                    u'气象站室外光照',u"回水压力",u'供水流量',u"燃气温度"],[u'一次回温度'],
                    [1],False,[1,2,3,4],[0])
    testVariable = BXY_1.main()