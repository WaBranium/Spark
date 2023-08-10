package sp230724

import java.io.ObjectOutputStream
import java.net.Socket


object driver {
  def main(args: Array[String]): Unit = {
    // TODO 建立网络连接客户端
    val client = new Socket("localhost", 9999)
    // TODO 建立OBJ输入流
    val inputStream = client.getOutputStream
    val objectOutputStream = new ObjectOutputStream(inputStream)
    // TODO 刷写Task OBJ (注意一定要序列化)
    objectOutputStream.writeObject(new Task)
    objectOutputStream.flush()
    // TODO 关闭
    objectOutputStream.close()
    client.close()
  }
}
