package sp230724

import java.io.ObjectInputStream
import java.net.ServerSocket

object executor {
  def main(args: Array[String]): Unit = {
    // TODO 创建服务端
    val server = new ServerSocket(9999)
    // TODO 创建接收的输入流
    val input = server.accept().getInputStream
    val objectInputStream = new ObjectInputStream(input)
    // TODO 接受对象 转换为Task类型
    val task = objectInputStream.readObject().asInstanceOf[Task]
    // TODO 编译执行
    val unit: List[Int] = task.compute()
    println(unit)
    // TODO 关闭
    objectInputStream.close()
    input.close()
    server.close()
  }
}
