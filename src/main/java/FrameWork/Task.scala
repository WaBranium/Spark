package FrameWork

/**
 * @author BiHan
 * 模拟分布式计算：RDD算子，封装数据获取方式及其计算逻辑
 * 因为涉及到对象传输所以切记要进行序列化
 */
class Task extends Serializable {

  // TODO 设计数据获取方式
  val datas: List[Int] = List(1, 2, 3, 4)

  // TODO 设计计算逻辑
  // 匿名函数写法  private val function: (Int) => Int = _ * 2
  val function = (num:Int)=>{num*2}

  // TODO 设计方法入口
  def compute ()={
    // 用变量代替传递逻辑，编译时会将function代码块替换为计算逻辑对象
    datas.map(function)
  }
}
