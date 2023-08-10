package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 换一种思路来进行word count
 * 切割为扁平数组后 使用spark特性 根据元组的key 来进行聚合操作
 */
object WordCount3 {
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

    // TODO 使用map进行元组整合为(word,1)的情况
    val sparkTruples = sparkWords.map(
      word => (word, 1)
    )

    // TODO 直接使用reduce by key 根据元组的key来进行聚合操作
    val wordCount = sparkTruples.reduceByKey(_ + _)
//    val wordCount = sparkTruples.reduceByKey(_ + _).map {
//    case (key, value) => {
//        key
//    }
//  }


    wordCount.collect().foreach(println)


    // TODO 运行完毕结束任务
    sparkContext.stop()
  }
}
