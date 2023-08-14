package FrameWork

import java.io.InputStream
import java.net.{ServerSocket, Socket}

/**
 * @author BiHan
 * 模拟分布式计算：Executor任务处理单元
 */
object Executor {
  def main(args: Array[String]): Unit = {
    // TODO 生成服务流
    val server: ServerSocket = new ServerSocket(9999)
    println("服务器启动成功，等待接收客户端数据")
    val client: Socket = server.accept()
    println("获取客户端，提取数据流")
    // TODO 获取客户端数据流
    val is: InputStream = client.getInputStream
    println("获取到数据流，开始处理任务")
    // TODO 进行任务处理
    val i: Int = is.read()
    println("获取到执行数据为：" + i)
    // TODO 关闭
    is.close()
    client.close()
    server.close()
  }

}
