package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 换一种思路来进行word count
 * 切割为扁平数组后 使用map来进行赋值
 */
object WordCount2 {
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

    // TODO 将各个元组整合成{word,[(word,1),(...)]} 的格式
    val sparkLists = sparkTruples.groupBy(
      item => item._1
    )

    // TODO 上面得到的是元组数组 要挨个list进行求和后放进map
    val wordCount = sparkLists.map {
      case (word, list) => {
        list.reduce(
          // 这里的t1 t2指代的是list里面的两个元组 采用匿名函数的方式进行累加 然后放到新元组中
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    wordCount.collect().foreach(println)


    // TODO 运行完毕结束任务
    sparkContext.stop()
  }
}
