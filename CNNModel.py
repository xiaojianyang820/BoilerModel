# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 17:41:36 2018

@author: hadoop
"""
import pickle,os
from SuperParasReader import SuperParasReader
from sklearn import preprocessing
import numpy as np
import torch
from torch import nn,optim
from torch.autograd import Variable
import pandas as pd
import datetime

superParasDict = SuperParasReader()


class CNNModel(object):
    def __init__(self,dataFrame,featureColumns,targetColumn):
        # 加载历史数据集
        self.dataFrame = dataFrame
        # 检查是否有数据正则化函数
        self.haveScalerLabel = self.haveScaler()
        # 检查是否有预训练的模型参数文件
        self.havePreTrainedParas = self.havePreTrainedRecord()
        # 指定特征和目标
        self.featureColumns = featureColumns; self.targetColumn = targetColumn
        # 初始化模型结构
        inChannel = superParasDict['inChannel'] + 1
        outChannel = superParasDict['outChannel']
        step = superParasDict['step']
        hidden = superParasDict['hidden']
        output = superParasDict['output']
        upLag = superParasDict['upLag']
        self.model = CNNModel_Kernal_2(inChannel,outChannel,step,hidden,upLag,output).cuda()
        if self.havePreTrainedParas:
            print u'模型正在加载历史结构参数'
            self.model.load_state_dict(torch.load(superParasDict['ModelParasFile']))
        
    def haveScaler(self):
        try:
            with open(superParasDict['FScaler'],'r') as f:
                self.featureScaler = pickle.load(f)
            with open(superParasDict['TScaler'],'r') as f:
                self.targetScaler = pickle.load(f)
            return True
        except:
            return False
    
    def havePreTrainedRecord(self):
        return os.path.exists(superParasDict['ModelParasFile'])
            
    def transDataFrame(self,dataFrame,feature,target,scalerLabel):
        hour = []
        for item in dataFrame.index:
            hour.append(item.hour)
        hour = np.array(hour).reshape(-1,1)
        
        if not self.haveScalerLabel:
            self.featureScaler = preprocessing.StandardScaler()
            self.targetScaler = preprocessing.StandardScaler()
            self.featureScaler.fit(feature)
            self.targetScaler.fit(target)
            with open(superParasDict['FScaler'],'w') as f:
                pickle.dump(self.featureScaler,f)
            with open(superParasDict['TScaler'],'w') as f:
                pickle.dump(self.targetScaler,f)
            self.haveScalerLabel = True
        if scalerLabel:
            feature = self.featureScaler.transform(feature)
            target = self.targetScaler.transform(target)
        feature = np.hstack((feature,hour))
        newFeature = []
        newTarget = []
        featureT = feature.T
        upLag = superParasDict['upLag']
        for k in range(upLag,feature.shape[0]):
            newFeature.append(featureT[:,k-upLag:k])
            newTarget.append(target[k,0])
        return np.array(newFeature),np.array(newTarget)
        
    def isNull(self,Feature):
        validIndex = []
        for k,item in enumerate(Feature):
            if np.isnan(item.ravel()).sum() < 5:
                validIndex.append(k)
        return np.array(validIndex)
        
        
    def preprocess(self):
        self.dataFrame = self.dataFrame.fillna(method='ffill',limit=100).fillna(
                            method='bfill',limit=100)
        NoNull_dataFrame = self.dataFrame.fillna(method='ffill').fillna(method='bfill')
        NoNull_feature = NoNull_dataFrame[self.featureColumns].as_matrix()
        NoNull_target = NoNull_dataFrame[self.targetColumn].as_matrix()
        self.NoNull_feature = NoNull_feature
        self.NoNull_dataFrame = NoNull_dataFrame
        newFeature,newTarget = self.transDataFrame(NoNull_dataFrame,NoNull_feature,NoNull_target,True)
        feature = self.dataFrame[self.featureColumns].as_matrix()
        target = self.dataFrame[self.targetColumn].as_matrix()
        NullFeature,NullTarget = self.transDataFrame(self.dataFrame,feature,target,False)
        validIndex = self.isNull(NullFeature)
        print "由于数据缺失，抛弃掉的样本点数量：%d"%(len(NoNull_dataFrame) - len(validIndex),)
        return newFeature[validIndex],newTarget[validIndex]
        
    
    def trainModel(self,totalEpoch,corr):
        
        WD = superParasDict['weight_decay']
        LR = superParasDict['learningRate'] * corr
        
        if superParasDict['lossCriterion'] == 'L1':
            criterion = nn.L1Loss()
        elif superParasDict['lossCriterion'] == 'L2':
            criterion = nn.MSELoss()
        else:
            print u'需要指定误差函数类型'
            raise ValueError
        optimizer = optim.Adam(self.model.parameters(),lr=LR,weight_decay=WD)
        X,Y = self.preprocess()
        X = np.array(X,dtype=np.float32)
        Y = np.array(Y,dtype=np.float32)
        
        testNum = 5
        CUDA_X = torch.from_numpy(X[:-testNum]).cuda()
        CUDA_Y = torch.from_numpy(Y[:-testNum].reshape(Y.shape[0]-testNum,1,-1)).cuda()
        CUDA_XTEST = torch.from_numpy(X[-testNum:]).cuda()
        YTest = Y[-testNum:]
        Variable_X = Variable(CUDA_X)
        Variable_Y = Variable(CUDA_Y)
        Variable_XTEST = Variable(CUDA_XTEST)
        for epoch in range(totalEpoch):
            out = self.model(Variable_X)
            loss = criterion(out,Variable_Y)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if (epoch + 1)%100 == 0:
                print u'第%d次迭代循环后，剩余的拟合误差为：%.4f!'%(epoch,loss.data[0])
        print u'正在存储模型结构参数'
        torch.save(self.model.state_dict(),superParasDict['ModelParasFile'])
        self.model.eval()
        predict = self.model(Variable_XTEST).cpu().data.numpy()[:,0:1,0]
        predict = self.targetScaler.inverse_transform(predict)
        YTest = self.targetScaler.inverse_transform(YTest.reshape(-1,1))
        diff = np.mean(predict.ravel()) - np.mean(YTest.ravel())
        print u'模型预测值为%.2f,实际值是%.2f'%(np.mean(predict.ravel()),np.mean(YTest.ravel()))
        print u'模型当前的预测误差为%.2f'%diff
        
        return diff
    
    def predictModel(self,predictX,testLabel=False,testLength=0):
        # 产生新的日期索引
        currentIndex = list(self.NoNull_dataFrame.index)
        lastDate = currentIndex[-1]
        if testLabel:
            stopDate = lastDate + datetime.timedelta(0,60*testLength)
        else:
            stopDate = lastDate + datetime.timedelta(0,60*superParasDict['pLength'])
        startDate = lastDate + datetime.timedelta(0,60)
        newIndex = pd.date_range(start=startDate,end=stopDate,freq='1min')
        # 合并虚拟数据和实际数据
        newDataFrame = pd.DataFrame(predictX,index=newIndex,columns=self.featureColumns)
        validDataFrame = self.NoNull_dataFrame[self.featureColumns]
        newDataFrame = pd.concat([validDataFrame,newDataFrame])
        newDataFrame = newDataFrame.fillna(method='ffill').fillna(method='bfill')

        # 对数据结构进行转换
        newpredictX = newDataFrame[self.featureColumns].as_matrix()
        tempY = np.random.randn(newpredictX.shape[0],1)
        
        newpredictX,tempY = self.transDataFrame(newDataFrame,newpredictX,tempY,True)
        # 预测
        newpredictX = np.array(newpredictX,dtype=np.float32)
        Variable_PredictX = Variable(torch.from_numpy(newpredictX).cuda())
        predictY = self.model(Variable_PredictX)
        predictY = predictY.cpu().data.numpy()
        predictY = predictY.reshape(predictY.shape[0],predictY.shape[2])
        predictY = self.targetScaler.inverse_transform(predictY)
        
        return predictY[-predictX.shape[0]:]
        
    
class CNNModel_Kernal(nn.Module):
    def __init__(self,inChannel,outChannel,step,hidden,upLag,outPut):
        super(CNNModel_Kernal,self).__init__()
        self.layer_1 = nn.Sequential()
        self.layer_1.add_module("Conv_1",nn.Conv1d(inChannel,outChannel,step,step,padding=0))
        self.layer_2 = nn.Sequential(
                                     nn.Linear(int(outChannel*(upLag-0)/float(step)),hidden),nn.ReLU(True))
        self.layer_3 = nn.Linear(hidden,outPut)
        
    def forward(self,x):
        x = self.layer_1(x)
        # 序列数seq，卷积输出channel，序列经过卷积后得到的积分量特征features
        s,c,f = x.size()
        x = x.view(s,c*f)
        x = self.layer_2(x)
        x = self.layer_3(x)
        x = x.view(s,1,-1)
        return x
        
class CNNModel_Kernal_2(nn.Module):
    def __init__(self,inChannel,outChannel,step,hidden,upLag,outPut):
        super(CNNModel_Kernal_2,self).__init__()
        self.inChannel = inChannel
        for i in range(inChannel):
            execStr = "self.layer_1%d = nn.Conv1d(1,1,step,step,padding=0)"%i
            exec(execStr)
        self.layer_2 = nn.Sequential(
                                     nn.Linear(int(inChannel*(upLag)/float(step)),hidden),nn.ReLU(True))
        self.layer_3 = nn.Linear(hidden,outPut)
        
    def forward(self,x):
        subX = []
        for i in range(self.inChannel):
            partX = x[:,i:i+1,:]
            execStr = 'partXOut_%d = self.layer_1%d(partX)'%(i,i)
            exec(execStr)
            exec('s,c,f = partXOut_%d.size()'%i)
            exec('partXOut_%d = partXOut_%d.view(s,c*f)'%(i,i))
            exec('subX.append(partXOut_%d)'%i)
        subX = torch.cat(tuple(subX),1)
        X = self.layer_2(subX)
        X = self.layer_3(X)
        X = X.view(s,1,-1)
        return X
