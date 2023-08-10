package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {
  println("Creating the Obj")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val dataRDD1 = sc.makeRDD(List(("a", 1), ("c", 3)))
    val dataRDD2 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))

    val value: RDD[(String, (Iterable[Int], Iterable[Int]))] = dataRDD1.cogroup(dataRDD2)

    value.collect().foreach(println)


  }
}
