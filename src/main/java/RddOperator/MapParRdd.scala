package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapParRdd {

  def main(args: Array[String]): Unit = {
    //TODO 建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapFunction")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 建立内存型RDD行动算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))



    //TODO 调用接口 Map传参
    //数据执行顺序：分区内有序 分区间无需
    //可以以分区为单位进行数据转换操作
    //但是会将整个分区的数据加载到内存进行引用如果处理完的数据是不会被释放掉，存在对象的引用。
    //在内存较小，数据量较大的场合下，容易出现内存溢出。
    //看作是Map的批处理接口
    val value: RDD[Int] = Rdd.mapPartitions(
      iter => {
        iter.map(_*2)
      }
    )

    value.collect().foreach(println)
    sc.stop()
  }

}
