package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GlomRdd {

  def main(args: Array[String]): Unit = {
    // TODO 创建配置建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Glom")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 建立算子
    //  glom 是把同一分区中的数据合并成一个数组的转换算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),numSlices = 2)
    val value: RDD[Array[Int]] = Rdd.glom()
    // 有个妙用就是求各个分区最大值
    val mapRdd: RDD[List[Int]] = value.map(
      list => {
        List(list.max)
      }
    )
    val numSum: Double = mapRdd.flatMap(
      data => data
    ).collect().sum
    println(numSum)

    // TODO 输出&关闭
    value.collect().foreach(data => println(data.mkString(",")))
    sc.stop()
  }

}
