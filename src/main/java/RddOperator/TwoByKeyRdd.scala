package RddOperator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 比较两个ByKey方法的性能
 */
object TwoByKeyRdd {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 获取 RDD
    val rdd = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 1)))
    val reduceRDD = rdd.groupByKey().map {
      case (word, iter) => {
        (word, iter.size)
      }
    }
    reduceRDD.collect().foreach(println)


    // 获取 RDD
    val rdd1 = sc.makeRDD(List(("a", 1), ("a", 1), ("a", 1), ("b", 1)))
    // 指定计算公式为 x+y
    val reduceRDD1 = rdd1.reduceByKey((x, y) => x + y)
    reduceRDD.collect().foreach(println)
    sc.stop()


    /**
     * TODO 在 groupbykey 实现过程中，由于groupbykey没有聚合功能，实现聚合计算是将所有数据分组完成后再进行聚合
     *  。而 reduceByKey 是有聚合功能的，实现过程中，在分组前也同样满足聚合条件（有相同的key，value能聚合），
     *  那么reduceByKey是不是在分组前就将数据先进行聚合了
     */

  }
}
