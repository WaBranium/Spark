package FrameWork.ComputeStandalone

import FrameWork.Task

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket


/**
 * @author BiHan
 * 模拟Spark分布式计算：客户端Driver层
 */
object Driver {
  def main(args: Array[String]): Unit = {
    // TODO 建立Spark RPC链接
    val client: Socket = new Socket("localhost", 9999)
    // TODO 生成数据流
    val os: OutputStream = client.getOutputStream

    // TODO 进行数据流逻辑处理
    val objs: ObjectOutputStream = new ObjectOutputStream(os)
    objs.writeObject(new Task)
    // TODO 发送数据流
    objs.flush()

    // TODO 关闭
    objs.close()
    client.close()
    println("客户端发送数据完毕")
  }
}
