# -*- coding: utf-8 -*-
"""
Created on Tue Apr 24 11:34:07 2018

@author: hadoop
"""

from MySQLConnector import MySQLConnector
import numpy as np
import pandas as pd

def evalueHFBT(realGasSeries,realHFBT,predictHFBT):
	# 评估一个控制周期内燃气量的波动，此变量的量纲是100
	realGasSeriesStd = np.std(realGasSeries)
	RGSS_Scalar = 50.0
	realGasSeriesStd = realGasSeriesStd/RGSS_Scalar
	print u'上一个控制周期，燃气序列的标准差为：%.4f'%realGasSeriesStd
	# 评估一个控制周期内预测回水温度和实际回水温度之间的相关性，此变量量纲为1
	RTP_Corr = np.corrcoef(predictHFBT,realHFBT)[0,1]
	RTPC_Scalar = 1.0
	RTP_Corr = RTP_Corr/RTPC_Scalar
	# 评估一个控制周期内预测回水温度和实际回水温度之间的均方误，此变量量纲为0.1
	RTP_MSE = np.mean((predictHFBT-realHFBT)**2)
	RTPM_Scalar = 0.1
	RTP_MSE = RTP_MSE/RTPM_Scalar
	# 一般来说，燃气标准差越大，越能容忍更高的误差
	score = min(max(1,realGasSeriesStd),1.5)*(0.2*RTP_Corr + 0.8*(1-min(RTP_MSE,1)))
	print u'上一个控制周期，预测曲线准确率评分为：%.4f'%score
	return min(1,score)

def OutterChecker(predictGroups,SD,ED,HRZID,Code,downGasAmount,upGasAmount):
    # 基于起始和终止日期，建立合适的时间索引和DataFrame
    validIndex = pd.date_range(SD,ED,freq='1min')
    validDataFrame = pd.DataFrame(index = validIndex)
    # 创建数据库连接
    conn = MySQLConnector()
    conn.openConnector()
    # 查询该控制周期内燃气流量序列
    SQL_Gas = u'SELECT create_time,瞬时流量 FROM bd_xinkou_2 WHERE create_time>"%s" and create_time<"%s"'%(SD,ED)
    conn.cursor.execute(SQL_Gas)
    realGasSeries = np.array(conn.cursor.fetchall())
    if len(realGasSeries) == 0:
        return 1,[1],np.random.randn(len(validIndex)),np.random.randn(len(validIndex))
    realGasDataFrame = pd.DataFrame(np.array(realGasSeries[:,1],dtype=np.float32),index=realGasSeries[:,0],columns=[u'瞬时流量'])
    realGasDataFrame = realGasDataFrame.resample('1min').mean()
    validDataFrame[u'瞬时流量'] = realGasDataFrame[u'瞬时流量']
    realGasMean = realGasSeries[:,1].mean()
    # 查询换热站一次回温度
    SQL_HFBT = u'SELECT create_time,一次回温度 FROM bd_xinkou_hrz WHERE project_sub_station_id=%d and code="%s" and create_time>"%s" and create_time<"%s"'%(HRZID,Code,SD,ED)
    conn.cursor.execute(SQL_HFBT)
    realHFBTArray = np.array(conn.cursor.fetchall())
    if len(realHFBTArray) == 0:
        return 1,[1],np.random.randn(len(validIndex)),np.random.randn(len(validIndex))
    realHFBTSeries = pd.Series(np.array(realHFBTArray[:,1],dtype=np.float32),index=realHFBTArray[:,0],name=u'一次回温度')
    realHFBTSeries = realHFBTSeries.resample('1min').mean()
    validDataFrame[u'一次回温度'] = realHFBTSeries
    # 确定预测组的恰当索引
    try:
        assert realGasMean > downGasAmount
        assert realGasMean < upGasAmount
        predictGroupsIndex  = realGasMean//100 - downGasAmount//100
    except AssertionError:
        if realGasMean <= downGasAmount:
            predictGroupsIndex = 0
        else:
            predictGroupsIndex = -1
    targetPredict = predictGroups[predictGroupsIndex]
    validDataFrame[u'预测回水温度'] = targetPredict
    validDataFrame = validDataFrame.fillna(method='ffill').fillna(method='bfill')
    score = evalueHFBT(validDataFrame[u'瞬时流量'].as_matrix().ravel(),validDataFrame[u'一次回温度'].as_matrix().ravel(),
                        validDataFrame[u'预测回水温度'].as_matrix().ravel())
    GasDis = [1]
    print u'在该控制周期内实际耗费燃气量为：%.1f'%realGasMean
    return score,GasDis,validDataFrame[u'一次回温度'].as_matrix().ravel(),validDataFrame[u'预测回水温度'].as_matrix().ravel()
    