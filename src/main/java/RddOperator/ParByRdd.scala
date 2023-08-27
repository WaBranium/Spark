package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 *  出现一个机制叫隐式转换 从Rdd到PairRddFun
 *  具体内容在：
 */
object ParByRdd {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PartitionBy")
    val sc: SparkContext = new SparkContext(conf)

    val Rdd = sc.makeRDD(List(1, 2, 3, 4)).map( (_, 1) )
    /*
      隐式转换来自RDD.scala的伴生对象
      方法入口
      implicit def rddToPairRDDFunctions[K, V](rdd: RDD[(K, V)])
      (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDFunctions[K, V] = {
      new PairRDDFunctions(rdd)
    }
     */
    Rdd.partitionBy(new HashPartitioner(2))
      .glom()
      .collect()
      .foreach(data => println(data.mkString(",")))


    sc.stop()
  }

}
