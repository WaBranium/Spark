package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CoalesceRdd {

  def main(args: Array[String]): Unit = {
    // TODO 建立环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc = new SparkContext(conf)

    // TODO 建立算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    // TODO coalesce 缩减分区
    Rdd.coalesce(2)
      .glom()
      .foreach(data => {println(data.mkString(","))})

    println("================================")
    // TODO coalesce 扩大分区 需要使用shuffle
    Rdd.coalesce(4,true)
      .glom()
      .foreach(data => {
        println(data.mkString(","))
      })

    // TODO 停止
    sc.stop()
  }

}
