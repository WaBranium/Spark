package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortByRdd {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Sort")
    val sc = new SparkContext(conf)

    val Rdd: RDD[Int] = sc.makeRDD(List(21, 32, 6, 2, 3, 9))
    Rdd.sortBy(num => num)
      .collect()
      .foreach(println)

    sc.stop()
  }

}
