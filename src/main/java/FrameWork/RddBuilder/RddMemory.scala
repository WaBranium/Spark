package FrameWork.RddBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BiHan
 * 模拟实现内存Rdd
 */
object RddMemory {
  def main(args: Array[String]): Unit = {
    // TODO 启动运行环境 这里指定*是表明用上全部的核来为任务提供并行
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RddMemBuilder")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 生产内存数据 合成RDD
    val datas: Seq[Int] = Seq[Int](1, 2, 3, 4)
    // 注意 这里是套用的 sc.parallelize() 只不过用makeRDD更符合当前语境
    val RddMem: RDD[Int] = sc.makeRDD(datas)

    // TODO 打印输出 关闭环境
    RddMem.collect().foreach(println)
    sc.stop()


  }
}
