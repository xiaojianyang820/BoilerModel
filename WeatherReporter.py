# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 14:40:36 2018

@author: hadoop
"""
import numpy as np

def weatherPredictor(averWeather,startTime,pLength):
    weatherSeasonality = []
    for item in [u'温度',u'湿度',u'光照',u'风速']:
        weatherSeasonality.append(np.loadtxt(item + u'日间周期.txt').reshape(-1,1))
    weatherSeasonality = np.hstack(weatherSeasonality)
    seconds = startTime.hour*60 + startTime.minute
    currentLevel = weatherSeasonality[seconds]
    psudoWeather = []
    for i in range(seconds+1,seconds+1+pLength):
        remoteLevel = weatherSeasonality[i]
        psudoValue = averWeather - (currentLevel - remoteLevel)
        psudoWeather.append(psudoValue)
    return np.array(psudoWeather)

    