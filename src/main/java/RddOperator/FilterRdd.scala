package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FilterRdd {

  def main(args: Array[String]): Unit = {
    // TODO 创建运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Filter")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 建立算子并打印筛选结果
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6,7,8,9,10), 2)
    // 筛选并glom 就不需要落盘查看是否shuffle了
    Rdd.filter(_%2 == 0)
      .glom()
      .collect()
      .foreach(list => println(list.mkString(",")))
    // 从打印的结果看，看来并没有shuffle 依然是分区内进行

    // TODO 来做一个DEMO 筛选2015-05-17用户访问的URL并wordCount
    val Rdd1: RDD[String] = sc.textFile("{datas/apache.log}")
    Rdd1.filter(
      data =>{
        data.split(" ")(3).startsWith("17/05/2015")
      }
    ).map(
      data => {
        data.split(" ")(6)
      }
    ).groupBy(
      data => {
        data
      }
    ).map{
      case (url,iter)=>{
        (url,iter.size)
      }
    }.collect().foreach(println)

    // TODO 停止
    sc.stop()
  }

}
