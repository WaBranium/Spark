package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // TODO 加载Spark配置 读取数据文件
    // 加载配置为本地配置 设定任务名称为WordCount
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 设定配置到连接池中 建立连接
    val sparkContext = new SparkContext(sparkConf)

    // TODO 读取数据 进行扁平化处理
    // 加载数据 按行读取模式
    val sparkLines: RDD[String] = sparkContext.textFile("src/main/resources/*")
    // 扁平化处理 使用匿名函数 默认加载进去的变量进行切割处理
    // 此处使用匿名函数 _表示的是sparkLines中的每一行 这里对他的每行做切割操作 形成一个扁平数组
    val sparkWords = sparkLines.flatMap(
      _.split(" ")
    )

    // TODO 进行聚合操作 根据关键字形成[（Hello,Hello,Hello）,(...)]类似的元组list 并最终将List长度替换v加载到MAP中
    // 进行聚合操作
    val sparkTList = sparkWords.groupBy(
      // 前面word代表sparkWords这个list中的每一个元素 这里赋别名为word
      // 后面的word的意思是按照单词变量相等的规则进行聚合操作
      word => word
    )
    // 输出的格式为：
    // (2txt,CompactBuffer(2txt))
    // (Hello,CompactBuffer(Hello, Hello, Hello, Hello))
    // (world,CompactBuffer(world, world))
    // (Spark,CompactBuffer(Spark))

    // 形成[String -> list.size]的KV map
    val wcMap = sparkTList.map {
      // 模式匹配 代表如果是该类型 则放入该元素
      case (word, list) => {
        (word, list.size)
      }
    }
    wcMap.foreach(println)

    // TODO 运行完毕结束任务
    sparkContext.stop()
  }
}
