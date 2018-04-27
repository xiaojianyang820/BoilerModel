# BoilerModel
首先介绍几个程序文件的作用
* MainModel文件规定的自动控制系统的工作主流程，其图示由配图“集中供暖系统算法图示”
![集中供暖系统算法图示](https://github.com/xiaojianyang820/BoilerModel/blob/master/%E9%9B%86%E4%B8%AD%E4%BE%9B%E6%9A%96%E7%B3%BB%E7%BB%9F%E7%AE%97%E6%B3%95%E5%9B%BE%E7%A4%BA.png)
* MainModel对象的初始化过程中需要十一个位置参数和实现两个抽象方法。
  * reStartModel,bool。指明此次启动模型是热启动还是冷启动，如果是热启动，那么会加载原有的模型参数文件和正则化文件
