package RddOperator

import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 获取 RDD 笛卡尔积版本的zip
    val rdd = sc.makeRDD(List(("a", 1), ("b", 1), ("a", 3), ("a", 4), ("b", 2),("d",0) ), 2)
    val rdd1 = sc.makeRDD(List( ("a","23"),("b",35),("a",66),("c",2) ))

    rdd.join(rdd1).collect().foreach(println)
    println("===============================")
    rdd.leftOuterJoin(rdd1).collect().foreach(println)
    println("===============================")
    rdd.rightOuterJoin(rdd1).collect().foreach(println)
    println("===============================")
    // TODO 先在自己得rdd内融合 再与另外的rdd笛卡尔积运算
    rdd.cogroup(rdd1).collect().foreach(println)


    sc.stop()
  }

}
