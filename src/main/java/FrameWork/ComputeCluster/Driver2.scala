package FrameWork.ComputeCluster

import FrameWork.Task

import java.io.ObjectOutputStream
import java.net.Socket


/**
 * @author BiHan
 * 模拟完全分布式计算——驱动入口
 */
object Driver2 {
  def main(args: Array[String]): Unit = {
    // TODO 设定接收服务器端口
    val client1: Socket = new Socket("localhost", 9999)
    val client2: Socket = new Socket("localhost", 8888)

    // TODO 加载任务流，并以ObjOS来包裹
    val objos1: ObjectOutputStream = new ObjectOutputStream(client1.getOutputStream)
    val objos2: ObjectOutputStream = new ObjectOutputStream(client2.getOutputStream)

    // TODO 加载完整任务Task
    val task: Task = new Task
    // TODO 子任务容器承接Task的部分数据与计算逻辑
    val subRdd1: SubTaskRDD = new SubTaskRDD(task.datas.take(2), task.function)
    val subRdd2: SubTaskRDD = new SubTaskRDD(task.datas.takeRight(2), task.function)

    // TODO 发送子任务到各自的Executor
    println("服务端开始发送数据")
    objos1.writeObject(subRdd1)
    objos1.flush()
    objos2.writeObject(subRdd2)
    objos2.flush()

    // TODO 关闭客户端
    objos1.close()
    objos2.close()
    client1.close()
    client2.close()
  }
}
