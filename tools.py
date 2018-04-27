# -*- coding: utf-8 -*-
"""
Created on Mon Apr 16 10:50:10 2018

@author: hadoop
"""

import datetime,re,time

def tools_DateTimeTrans(ins_datetime):
    if isinstance(ins_datetime,datetime.datetime):
        return ins_datetime
    elif isinstance(ins_datetime,str):
        timePattern_1 = '(\d{2,4})-(\d{1,2})-(\d{1,2}) (\d{1,2}):(\d{1,2}):(\d{1,2})'
        if re.match(timePattern_1,ins_datetime):
            year,month,day,hour,minute,second = \
                                re.findall(timePattern_1,ins_datetime)[0]
            return datetime.datetime(int(year),int(month),int(day),
                                     int(hour),int(minute),int(second))
        else:
            raise ValueError(u'时间字符串格式不匹配')
    else:
        raise ValueError(u'时间对象格式不匹配')
            
class Timer(object):
    def __init__(self,startTime,speed,endTime="2018-03-26 00:00:00"):
        """
            startTime:str
                定时器开始的时间
            speed:str
                定时器运行速度
            endTime:str,optional
                定时器结束时间
        """
        # 将字符串格式的时间修正为标准的datetime型
        if len(startTime) == 10:
            self.startTime = datetime.datetime.strptime(startTime,"%Y-%m-%d")
        elif len(startTime) == 19:
            self.startTime = datetime.datetime.strptime(startTime,
                                                        "%Y-%m-%d %H:%M:%S")
        else:
            raise ValueError(u'The input format of time is not valid.\
                Try to transform it to "%Y-%m-%d" or "%Y-%m-%d %H:%M:%S"')
        # 记录生成该计时器对象时的系统时间
        self.realStartTime = datetime.datetime.now()
        # 记录生成该计时器对象时的系统时间与假定起始时间之间的差距
        self.delay = self.realStartTime - self.startTime
        # 计时器速度
        self.speed = speed
        
    def sleep(self,sleepTime):
        """
            sleepTime:Int
                程序睡眠时间
        """
        time.sleep(sleepTime/float(self.speed))
        
    def now(self):
        # 查询时实际系统时间
        current = datetime.datetime.now()
        # 经过时间
        delay = current - self.realStartTime
        # 模拟系统时间
        psudoNow = self.startTime + delay*self.speed
        year,month,day,hour,minute,second = \
            psudoNow.year,psudoNow.month,psudoNow.day,psudoNow.hour,psudoNow.minute,0
        return datetime.datetime(year,month,day,hour,minute,second)

class StandTimer(object):
    def now(self):
        current = datetime.datetime.now()
        year,month,day,hour,minute,second = current.year,current.month,current.day,\
                        current.hour,current.minute,0
        return datetime.datetime(year,month,day,hour,minute,second)
        
    def sleep(self,sleepTime):
        time.sleep(sleepTime)

        