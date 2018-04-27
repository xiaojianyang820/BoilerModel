# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 17:06:55 2018

@author: hadoop
"""
# 调用公共库
from abc import ABCMeta,abstractmethod
import os,datetime,copy
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

# 调用自有库
from SuperParasReader import SuperParasReader
from tools import tools_DateTimeTrans,Timer,StandTimer
from OutterChecker import OutterChecker

SuperParasDict = SuperParasReader()

class MainModel(object):
    
    __metaclass__ = ABCMeta
    
    def __init__(self,reStartModel,startDate,Clock,interval,ModelClass,
                 FeatureColumns,TargetColumns,initGasDis,testLabel,weatherIndex,
                 gasIndex):
        # 指明是否是重启模型
        self.reStartModel = reStartModel
        # 指明历史数据的起始时刻
        self.startDate = startDate
        # 该对象所使用的标准时钟
        self.Clock = Clock
        # 每一个训练阶段的长度
        self.interval = interval
        # 模型类
        self.ModelClass = ModelClass
        # 特征列
        self.featureColumns = FeatureColumns
        # 目标列
        self.targetColumns = TargetColumns
        # 初始燃气分配
        self.initGasDis = initGasDis
        # 是否对模型预测效果进行测试
        self.testLabel = testLabel
        # 天气列索引
        self.weatherIndex = weatherIndex
        # 燃气列索引
        self.gasIndex = gasIndex
    
    def removeOriginalFiles(self,*files):
        '''
            将原有的配置文件删除
            
            files: str
                配置文件路径
        '''
        for filename in files:
            if os.path.exists(filename):
                os.remove(filename)
            else:
                print u'[Warning]: %s 该文件不存在，请检查输入是否正确'%filename
                
    def splitTrainDate(self):
        """
            对全部历史数据进行划分，分阶段训练模型
            参数：无
            返回：list[tuple[datetime,datetime]]
                该list中包含多个tuple，每一个tuple由数据的起止时点构成
        """
        startDate = tools_DateTimeTrans(self.startDate)
        endDate = self.Clock.now()
        splitStages = []
        dayDelta = (endDate-startDate).days
        for k in range(dayDelta//self.interval):
            if len(splitStages) == 0:
                splitStages.append((startDate,startDate + 
                        datetime.timedelta(self.interval)))
            else:
                startSplit = splitStages[-1][1]
                splitStages.append((startSplit,startSplit + 
                        datetime.timedelta(self.interval)))
        lastSplitStart = splitStages[-1][1] - datetime.timedelta(
                            np.random.randint(low=self.interval//4,high=self.interval//2))
        splitStages.append((lastSplitStart,endDate))
        return splitStages
        
    def geneTestData(self,stageData,testLength):
        """
            产生指定长度的测试数据，对模型预测结果进行检验
            
            stageData:DataFrame
                该阶段历史数据
            testLength:int
                所需要加载的历史数据长度，以分钟为单位
        """
        startDate = stageData.index[-1] + datetime.timedelta(0,60)
        endDate = stageData.index[-1] + datetime.timedelta(0,60*testLength)
        testDataFrame = self.dataReader(startDate,endDate,24,'N1').readData()
        testDataFrame = testDataFrame.fillna(method='ffill').fillna(method='bfill')
        return testDataFrame[self.featureColumns].as_matrix(),testDataFrame[self.targetColumns].as_matrix()
    
    @abstractmethod
    def genePsudoWeather(self,stageData,weatherIndex,pLength):
        pass
    
    @ abstractmethod
    def dataReader(self):
        pass
    
    def genePsudoData(self,stageData,weather,GasDis):
        GasDis = np.array(GasDis)        
        
        feature = stageData[self.featureColumns].as_matrix()
        feature = stageData[self.featureColumns].fillna(method='ffill').fillna(method='bfill').as_matrix()
        PsudoData = np.array([feature[-1]]*SuperParasDict['pLength'])
        PsudoDatas = []
        if len(weather) > 1:
            PsudoData[:,self.weatherIndex] = weather
        for gasMount in range(SuperParasDict['downGasAmount'],SuperParasDict['upGasAmount'],100):
            tempPsudoData = copy.deepcopy(PsudoData)
            tempPsudoData[:,self.gasIndex] = np.array([GasDis]*len(tempPsudoData))*gasMount
            PsudoDatas.append(tempPsudoData)
        return PsudoDatas
        
    def InnerChecker(self,predictGroups):
        """
            该方法要确保预测集中高燃气量对应高回水温度
        """
        diffs = []
        for k in range(predictGroups.shape[0]-1):
            i = predictGroups[k]
            j = predictGroups[k+1]
            diff = j - i
            diffs.append(diff)
        diffs = np.array(diffs)
        return sum((diffs < -0.03).ravel())
        
    def geneTargetBT(self,dataFrame):
        dataFrame = dataFrame.fillna(method='ffill').fillna(method='bfill')
        # 读取当前的时间和当前的室外温度
        currentTime = dataFrame.index[-1]
        currentHour = currentTime.hour
        currentOutTemp = dataFrame[u'气象站室外温度'].as_matrix().ravel()[-1]
        
        outT2BT_Data = []
        index = dataFrame.index
        outT = dataFrame[u'气象站室外温度'].as_matrix().ravel()
        BT = dataFrame[self.targetColumns].as_matrix().ravel()
        for k,i in enumerate(index):
            outT2BT_Data.append([i.hour,outT[k],BT[k]])
            
        def specificHourMap(hour):
            def func(x,a,b):
                return a*x + b
            
            specificHourData = []
            for item in outT2BT_Data:
                if item[0] == hour:
                    specificHourData.append([item[1],item[2]])
            specificHourData = np.array(specificHourData)
            paras,COV = curve_fit(func,specificHourData[:,0],specificHourData[:,1],p0=(0,0))
            print "参数估计时的协方差矩阵："
            print COV
            validFunc = lambda x: paras[0] * x + paras[1]
            return validFunc
        
        MapFunc = specificHourMap(int(currentHour+(SuperParasDict['pLength']//60)/2.0)%24)
        return MapFunc(currentOutTemp),BT[-1]
        
    def geneValidGas(self,predictGroups,pursueBT,stableBT):
        down = SuperParasDict['downGasAmount']
        minPursueIndex = np.argmin(np.abs(np.mean(predictGroups[:,-3:],axis=1) - pursueBT))
        pursueGas = down + minPursueIndex * 100
        minStableIndex = np.argmin(np.abs(np.mean(predictGroups[:,-3:],axis=1) - stableBT))
        stableGas = down + minStableIndex * 100
        return pursueGas,stableGas
    
    def main(self):
        if self.reStartModel:
            # 首先将原有的正则化文件和模型参数文件删除
            self.removeOriginalFiles(SuperParasDict['FScaler'],
                                     SuperParasDict['TScaler'],
                                     SuperParasDict['ModelParasFile'])
            # 对已有历史数据进行分组
            splitStages = self.splitTrainDate()
            assert type(splitStages) == list
            assert len(splitStages) > 0
            assert type(splitStages[0][1]) == datetime.datetime
            # 根据每一段时期的训练数据来训练模型
            for k,dates in enumerate(splitStages):
                SD,ED = dates
                print u'正在加载历史数据（%s-%s）'%(SD,ED)
                stageData = self.dataReader(SD,ED,SuperParasDict['HRZID'],
                                            SuperParasDict['Code']).readData()
                print u'该阶段历史数据加载结束'
                print u'开始初始化模型'
                self.model = self.ModelClass(stageData,self.featureColumns,self.targetColumns)
                print u'对模型进行训练'
                diff = self.model.trainModel(300,1)
        else:
            diff = 0.0
            
        print 
        # 初始化一些必要参数
        corr = 0 # 上一控制周期的效果
        retryNums = 3 # 如果预测结果组没有通过事前检测，可以重新训练的次数
        controlLoops = 1 # 记录控制周期
        GasDis = self.initGasDis # 如果存在多台锅炉，那么锅炉间对总燃气量的分配
        #while True:
        for t in range(100):
            print u'开始第%d次系统控制'%controlLoops
            currentTime = self.Clock.now()
            startTime = currentTime - datetime.timedelta(np.random.randint(9,15))
            print u'对当前阶段历史数据进行加载(%s--%s)'%(startTime,currentTime)
            stageData = self.dataReader(startTime,currentTime,SuperParasDict['HRZID'],
                                        SuperParasDict['Code']).readData()
            self.model = self.ModelClass(stageData,self.featureColumns,self.targetColumns)
            print u'对模型进行训练'
            
            diff = self.model.trainModel(200,(1-corr))
            # 根据测试数据集进行模型检验，只适用于回测阶段
            if self.testLabel:
                print u'多模型结果进行检验'
                testFeature,testTarget = self.geneTestData(stageData,60*48)
                predictTarget = self.model.predictModel(testFeature,self.testLabel,60*48)
                figure = plt.figure()
                ax = figure.add_subplot(111)
                ax.plot(predictTarget.ravel(),c='blue',lw=2,alpha=0.6,label='Prediction')
                ax.plot(testTarget.ravel(),c='red',lw=1.5,alpha=0.7,label='Reality')
                ax.legend(loc='best')
                figure.savefig(u'测试结果/当前模型预测结果（%s）.png'%currentTime,dpi=300)
            # 结合虚拟数据进行预测
            print u'正在产生虚拟数据集'
            #    产生虚拟的天气预报
            PsudoWeather = self.genePsudoWeather(stageData,self.weatherIndex,
                                                 SuperParasDict['pLength'])
            assert PsudoWeather == [-1] or PsudoWeather.shape == (SuperParasDict["PLength"],
                                                                        4)
            #    产生虚拟的数据集
            PsudoDatas = self.genePsudoData(stageData,PsudoWeather,GasDis)
            predictGroups = []
            print u'对虚拟数据集进行预测'
            for PsudoData in PsudoDatas:
                print '.',
                predictGroup = self.model.predictModel(PsudoData)
                predictGroups.append(predictGroup)
            print 
            predictGroups = np.hstack(predictGroups).T
            # 根据预测偏置误差调整预测结果组
            predictGroups = predictGroups - diff
            if self.InnerChecker(predictGroups) <= 200 or retryNums == 0:
                # 重置重新训练次数
                retryNums = 3
                print u'正在计算最恰当的目标回水温度'
                targetBT,stableBT = self.geneTargetBT(stageData)
                print u'目标回水温度：%.2f,稳定回水温度:%.2f'%(targetBT,stableBT)
                pursueBT = SuperParasDict['stableWeight']*stableBT + \
                            SuperParasDict['targetWeight']*max(min(targetBT,stableBT+2),stableBT-2)
                print u'根据权值分配，最合适的追踪回水温度为：%.2f'%pursueBT
                pursueGas,stableGas = self.geneValidGas(predictGroups,pursueBT,stableBT)
                print u'追踪回水温度所需要的燃气量为：%d,保持回水温度所需要的燃气量为：%d'%(pursueGas,stableGas)
                np.savetxt('PredictGroups/第%d个控制周期预测.txt'%controlLoops,predictGroups,fmt='%.2f')
                
                endTime = currentTime + datetime.timedelta(0,60*SuperParasDict['pLength'])
                while True:
                    cc = self.Clock.now()
                    if (endTime - cc).seconds < 20 or cc > endTime:
                        break
                    self.Clock.sleep(10)
                print 
                print u'对模型进行事后检验'
                corr,GasDis,rBT,pBT = OutterChecker(predictGroups,currentTime+datetime.timedelta(0,60),
                                                endTime,SuperParasDict['HRZID'],
                                                SuperParasDict['Code'],SuperParasDict['downGasAmount'],
                                                SuperParasDict['upGasAmount'])
                self.plotPredictGroups(predictGroups,controlLoops,rBT,pBT)
                controlLoops += 1
                if corr > 0.9:
                    corr = 1
                print u'+++++++++++++++++++++++++++++++++++++++++++++'
                plt.close('all')
            else:
                print u'未通过事前检验，需要重新训练'
                retryNums -= 1
                continue
    
    def plotPredictGroups(self,predictGroups,controlLoops,rBT,pBT):
        figure = plt.figure()
        ax = figure.add_subplot(111)
        for i in predictGroups:
            ax.plot(i,lw=0.8,c='r',ls='--')
        ax.plot(rBT+np.random.randn(len(rBT))*0.03,lw=1.5,c='k',ls='-',label='Reality')
        ax.plot(pBT,lw=1.5,c='blue',ls='-',label='Prediction')
        ax.legend(loc='best')
        figure.savefig('PredictGroups/第%d个控制周期预测.png'%controlLoops,dpi=300)
        plt.close('all')
            
        
            
        