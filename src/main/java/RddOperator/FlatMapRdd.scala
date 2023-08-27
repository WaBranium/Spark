package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapRdd {

  def main(args: Array[String]): Unit = {
    // TODO 建立配置文件创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("FlatMap")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 创建算子
    val Rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2, 3, 4), List(5, 6, 7, 8)))
    // 执行结果是1 2 5 6 表明其实是先对最外围的List里面的元素进行操作
    // 两个list先take2出来 在合并放到一起 本质也是对元素进行操作
    // 所以flatMap其实是先map再flat
    val value: RDD[Int] = Rdd.flatMap(
      list => {
        list.take(2)
      }
    )
    // 如果最外围数据元素并不完全相同 如何解决？
    // 利用模式匹配进行处理
    val Rdd1: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, 4, 5))
    // 反正最后返回类型要统一才行
    val value1: RDD[Any] = Rdd1.flatMap(
      elem => {
        elem match {
          case list: List[Int] => list.take(1)
          case dat => List(dat)
        }
      }
    )

    // TODO 运行&关闭
    value.collect().foreach(println)
    println("=============================")
    value1.collect().foreach(println)
    sc.stop()
  }

}
