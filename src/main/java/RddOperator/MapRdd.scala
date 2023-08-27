package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapRdd {

  def main(args: Array[String]): Unit = {
    //TODO 建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapFunction")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 建立内存型RDD行动算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //TODO 建立rdd map处理函数 注意：元素的输入类型与返回类型必须与Rdd内的数据元素类型一样
    def mapFunction (num:Int):Int ={
      num*2
    }

    //TODO 调用接口 Map传参
    // 数据执行顺序：分区内有序 分区间无需
    val value: RDD[Int] = Rdd.map(mapFunction)
    value.collect().foreach(println)
    sc.stop()
  }
}
