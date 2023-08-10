package sp230724

class Task extends Serializable {
  //制造变量
   val list = List(1, 2, 3, 4)
  // 逻辑对象 需要声明输入输出
   val logic:(Int)=>Int = _*2

  // 一定要声明好返回类型 不然要出问题
  def compute (): List[Int] ={
    println(list)
    // 最后一行默认为返回类型
    list.map(logic)
  }

}
