package FrameWork.ComputeCluster

class SubTaskRDD extends Serializable {

  // TODO 设计数据容器等待盛放数据
  var datas : List[Int] = _

  // TODO 设计计算容器等待接收计算逻辑
  var target: (Int)=>Int = _

  // 构造器
  def this(datas:List[Int],target:(Int)=>Int) = {
      this()
      this.datas = datas
      this.target = target
  }



  // TODO 设置方法执行入口
  def compute() = {
    datas.map(target)
  }

}
