package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctRdd {
  def main(args: Array[String]): Unit = {
    // TODO 建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Distinct")
    val sc = new SparkContext(conf)

    // TODO 建立算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    // map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1) 去重本质也是聚合
    val value: RDD[Int] = Rdd.distinct()

    // TODO 打印&输出
    value.collect().foreach(println)
    sc.stop()
  }

}
