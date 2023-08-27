package RddOperator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.sparkproject.jetty.servlet.listener.ELContextCleaner
object MapParInRdd {

  def main(args: Array[String]): Unit = {
    //TODO 建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MapFunction")
    val sc: SparkContext = new SparkContext(conf)

    //TODO 建立内存型RDD行动算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)


    //TODO 调用接口 Map传参
    // 带索引的分区批处理操作，可以用来根据索引筛选数据
    val value: RDD[Int] = Rdd.mapPartitionsWithIndex(
      (index,iter) => {
        if (index == 1){
           iter.map(_ * 2)
        }else{
          Nil.iterator
        }
      }
    )

    value.collect().foreach(println)
    sc.stop()
  }
}
