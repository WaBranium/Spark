package FrameWork.ComputeCluster

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author BiHan
 * 模拟分布式计算：Executor任务处理单元
 */
object Executor2 {
  def main(args: Array[String]): Unit = {
    // TODO 生成服务流
    val server: ServerSocket = new ServerSocket(8888)
    println("服务器启动成功，等待接收客户端数据")
    val client: Socket = server.accept()
    println("获取客户端，提取数据流")
    // TODO 获取客户端数据流
    val is: InputStream = client.getInputStream
    val obji: ObjectInputStream = new ObjectInputStream(is)
    println("获取到数据流，开始处理任务")
    // TODO 进行任务处理
    val task: SubTaskRDD = obji.readObject().asInstanceOf[SubTaskRDD]
    val values: List[Int] = task.compute()
    println("获取到执行数据为：" + values)
    // TODO 关闭
    is.close()
    client.close()
    server.close()
  }

}
