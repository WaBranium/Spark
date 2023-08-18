package FrameWork.RddBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RddFile {

  def main(args: Array[String]): Unit = {
    // TODO 创建Spark运行环境
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 读取文件数据 构建文件类型数据行动算子
    val fileRDD: RDD[String] = sc.textFile("{datas/*}")

    // TODO 打印数据 关闭
    fileRDD.collect().foreach(println)
    sc.stop()
  }

}
