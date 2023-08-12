package HelloWorld

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {


  def main(args: Array[String]): Unit = {
    // TODO 创建Spark运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context: SparkContext = new SparkContext(sparkConf)

    // TODO 读取文件
    val lines: RDD[String] = context.textFile("{datas/*}")

    // TODO 转换形态，开始业务逻辑
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 转换为Tuple形态
    val values: RDD[(String, Int)] = words.map(
      (word: String) => {
        (word, 1)
      }
    )
    // 聚合
    val tuples: RDD[(String, Iterable[(String, Int)])] = values.groupBy(_._1)

    // list内部进行计算
    val wordToCount: RDD[(String, Int)] = tuples.map {
      case (str, list) => {
        // reduce不能改变返回元素的类型 这里list单个是tuple 所以返回也必须是Tuple
        val tuple: (String, Int) = list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }

        )
        tuple
      }
    }

    // TODO 输出 停止
    wordToCount.collect().foreach(println)
    context.stop()


  }

}
