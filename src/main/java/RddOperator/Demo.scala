package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {

  def main(args: Array[String]): Unit = {
    // TODO 建立环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Demo")
    val sc = new SparkContext(conf)

    // TODO 开始转换
    val textRdd: RDD[String] = sc.textFile("{datas/agent.log}")
    textRdd.map(
        // TODO 根据(省份，广告)两个进行wordCount
      data => {
        val datas: Array[String] = data.split(" ")
        ((datas(1), datas(4)), 1)
      }
    ).reduceByKey(_+_)
      .map{
        // TODO 转变为 （省份，（广告，数量））
        case ((pro,ad),num) => {
          (pro,(ad,num))
        }
      }.groupByKey() // TODO 根据省份，将（广告，数量）聚合为一起成为 （省份,iter[(广告，数量)]），要进行排序筛选
       .mapValues(
         // TODO 挑选前三 先把容器转换为队列再排序
        data => {
              data.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        }
      )
      .collect()
      .foreach(println)

    sc.stop()
  }

}
