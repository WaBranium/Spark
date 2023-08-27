package RddOperator

import org.apache.spark.{SparkConf, SparkContext}

object DoubleVRdd {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("doubleValue")
    val sc = new SparkContext(conf)

    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))

    // TODO 求交集
    println(dataRDD1.intersection(dataRDD2)
      .collect()
      .mkString(","))

    // TODO 求并集
    println(dataRDD1.union(dataRDD2)
      .collect()
      .mkString(","))

    // TODO 求差集
    println(dataRDD1.subtract(dataRDD2)
      .collect()
      .mkString(","))

    // TODO tips: 注意 以上三个方法对数据类型有强制要求 必须完全一致

    // TODO 做拉链（可不是拉链表）
    //  这个对数据类型没强制要求 但是对两个数据Rdd的分区数以及元素总数有强一致要求
    println(dataRDD1.zip(dataRDD2)
      .collect()
      .mkString(","))


    sc.stop()
  }

}
