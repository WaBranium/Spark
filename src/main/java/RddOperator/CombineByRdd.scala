package RddOperator

import org.apache.spark.{SparkConf, SparkContext}

/**
 *  TODO 其实前面几种聚合算子都是封装的这个
 *    只不过传参不一样，具体可以看:https://blog.csdn.net/zqm_super_man/article/details/110551101
 *    1.reduceByKey: 相同key的第一个数据不进行任何计算，分区内和分区间计算规则相同
 *    2.foldByKey: 相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
 *    3.AggregateByKey：相同key的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
 *    4.CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同
 */
object CombineByRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 获取 RDD
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 1), ("a", 3), ("a", 4), ("b", 2)), 2)

    rdd.combineByKey(
      // TODO 将iter[T]中首个数据的数据结构进行转换
      num => (num,1),
      // TODO 分区内运算
      (t1:(Int,Int),v) => (t1._1 + v , t1._2+1),
      // TODO 分区间运算
      (t1:(Int,Int),t2:(Int,Int)) => (t1._1 + t2._1 , t1._2 + t2._2)
    ).collect()
      .foreach(println)

    sc.stop()
  }

}
