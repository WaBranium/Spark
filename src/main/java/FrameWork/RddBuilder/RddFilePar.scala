package FrameWork.RddBuilder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RddFilePar {


  def main(args: Array[String]): Unit = {
    // TODO 创建Spark运行环境
    // 创建 Spark 运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    // TODO 读取文件数据 构建文件类型数据行动算子
    /*
      判断文件类型行动算子的分区方式
      TextFile里面封装的是HadoopFile 表明用Hadoop读取文件的方式进行分区（这里有个伏笔 1.1原则）
      其中HadoopFile的方法里面
      def hadoopFile[K, V](
        path: String,
        inputFormatClass: Class[_ <: InputFormat[K, V]],
        keyClass: Class[K],
        valueClass: Class[V],
        minPartitions: Int = defaultMinPartitions)
       里面的默认最小分区是这样来看：
       def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
       而defaultParallelism就是老演员了 来自 taskScheduler.defaultParallelism
       当然我们也可以自己指定miniPartitions 就不会再涉及比较了
       之后的分区数制定规则是：int(文件总字节长度/最小分区数) 得出每个分区该拿到多少字节
       再通过 文件总字节长度/每个分区该拿到多少字节 取余数 如果余数大于 分区可容纳字节的0.1倍 就会开一个新的分区
       这就是所谓的 1.1原则 就是取余判断 舍or入 的问题
       具体详细过程可以看 val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
       进入FileInputFormat 找到getSplits查看
     */
    val fileRDD: RDD[String] = sc.textFile("{datas/*}",2)

    // TODO 打印数据 关闭
   /*
   实际上 在保存数据的时候 会按照偏移量来读取数据 并且前后都闭
   但是由于Hadoop读取文件是按行读取的方式 比如说 0号分区里[0,7] 1号[7.14] 但是第一行有8个偏移量
   那1号分区下次就只能从偏移量9开始读了
    */
    fileRDD.collect().foreach(println)

    sc.stop()
  }


}
