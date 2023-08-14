package FrameWork

import java.io.OutputStream
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
    os.write(6)

    // TODO 发送数据流
    os.flush()

    // TODO 关闭
    os.close()
    client.close()
  }
}
