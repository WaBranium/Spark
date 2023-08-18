package FrameWork.RddBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object RddWholeTextFile {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark运行环境
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 以文件为单位读取数据 区别于TextFile以行为单位来读取数据 元组的key是文件路径 value是文件内容
    val fileRDD: RDD[(String, String)] = sc.wholeTextFiles("{datas/*}")

    // TODO 打印数据 关闭
    fileRDD.collect().foreach(println)
    sc.stop()
  }

}
