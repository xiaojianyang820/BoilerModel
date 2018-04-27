# BoilerModel
首先介绍几个程序文件的作用
* MainModel文件规定的自动控制系统的工作主流程，其图示由配图“集中供暖系统算法图示”
![集中供暖系统算法图示](https://github.com/xiaojianyang820/BoilerModel/blob/master/%E9%9B%86%E4%B8%AD%E4%BE%9B%E6%9A%96%E7%B3%BB%E7%BB%9F%E7%AE%97%E6%B3%95%E5%9B%BE%E7%A4%BA.png)
* MainModel对象的初始化过程中需要十一个位置参数和实现两个抽象方法。
  * reStartModel,bool。指明此次启动模型是热启动还是冷启动，如果是热启动，那么会加载原有的模型参数文件和正则化文件；如果是启动，那么会删除原有的模型参数文件和正则化文件，重新学习。
  * startDate,datetime。表明控制系统可用历史数据的起点。
  * Clock,自定义对象。用于在控制系统中起到时钟的作用，它是一个自定义对象,应该有两个方法，第一个方法是now()方法，返回当前时钟时刻，第二个方法是sleep(n)，程序会终止n秒。
  * interval,int。分阶段训练过程中每一个阶段的天数。
  * ModelClass,自定义类。该类的实例化参数为历史数据DataFrame，特征列FeatureColumns和目标列TargetColumns。基于这些数据，构建的对象应该有两个核心方法，第一个是trainModel方法，参数为学习强度和上一轮次学习准确率；第二个是testModel方法，有一个必选参数，待预测数据Array，两个可选参数，是否是测试testLabel，以及待测试数据的长度，以分钟为单位，如果不是测试，那就是预测，预测数据的长度来自于配置文件superParas.txt。
  * FeatureColumns,list。用作模型特征的列。
  * TargetColumns,list。用于模型预测目标的列。
  * initGasDis,list。初始化的燃气分配策略，由于某一些项目中存在多台锅炉的情况，需要把总燃气量分配到各个锅炉，初始的分配方案由参数指定，后续的分配方案由锅炉实际运营状况决定。
  * testLabel,bool。是否启用模型测试功能，该功能只适用于回测阶段。
  * weatherIndex,list。特征列中有关天气的元素索引，顺序为气温，湿度，光照和风速。
  * gasIndex,list。特征列中有关燃气的元素索引。
  * 第一个抽象方法是genePsudoWeather，它接受参数历史数据DataFrame，天气数据列索引list，和预测长度int，返回构造好的未来一个控制周期的预测天气数据。
  * 第二个抽象方法是dataReader，它接受历史数据的起始日期，结束日期，以为换热站编号，和板换机组编号，返回一个带有readData方法的对象，调用该方法可以返回相应的历史数据。
  * 此外MainModel中还有两个重要的方法，一个是产生目标回水温度的方法，它对确定恰当的燃气量比较重要，一个是对预测结果进行事后检验的方法，它决定了模型预测结果如何进而决定了下一个控制周期的学习强度。
