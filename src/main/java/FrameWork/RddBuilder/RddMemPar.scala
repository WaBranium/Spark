package FrameWork.RddBuilder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BiHan
 * 通过RddMemory的demo 展示分区的情况
 * 注意区分 分区 和 并行的概念 分区可能导致并行与并发
 * 当前任务可接受的最大并行度仅与配置了多少CPU的核有关
 */
object RddMemPar {
  def main(args: Array[String]): Unit = {
    // TODO 建立运行环境
    // 建立运行配置 将CPU满核打满
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CheckoutPartitions")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 使用内存数据 建立内存型RDD行动算子
    /*
    makeRdd里面默认并行度在
       def makeRDD[T: ClassTag](
                                 seq: Seq[T],
                                 numSlices: Int = defaultParallelism): RDD[T] = withScope {
         parallelize(seq, numSlices)
       }
    点击 defaultParallelism 发现是由任务调度器给的 taskScheduler.defaultParallelism
    通过taskScheduler找到实现类的实现类 LocalSchedulerBackend
       override def defaultParallelism(): Int =
         scheduler.conf.getInt("spark.default.parallelism", totalCores) 其中 totalcores代表着现在cpu核的分配数
    如果想指定分配核数 可以通过 conf.set("spark.default.parallelism","你要分配的核数") 来进行指定
    分区原则： 通过makeRDD源码可以定位到这里：
       override def getPartitions: Array[Partition] = {
         val slices = ParallelCollectionRDD.slice(data, numSlices).toArray
         slices.indices.map(i => new ParallelCollectionPartition(id, i, slices(i))).toArray
       }
    点击 ParallelCollectionRDD.slice 可以看到seq.match 我们这里是Seq类型为默认算法 定位到positions方法
    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
         (0 until numSlices).iterator.map { i =>
           val start = ((i * length) / numSlices).toInt
           val end = (((i + 1) * length) / numSlices).toInt
           (start, end)
         }
       }
    解析：将现在的分区数划分为[0,..,numSlices-1]的各个分区id
    然后 对于各个分区id里面取数据区的index在[left,right)的数据放到自己的分区 至于索引为什么左开右闭 看这里
    回到 array.slice(start, end).toSeq 点击
   override def slice(from: Int, until: Int): Array[T] = {
        val reprVal = repr
        val lo = math.max(from, 0)
        val hi = math.min(math.max(until, 0), reprVal.length)
        val size = math.max(hi - lo, 0)
        val result = java.lang.reflect.Array.newInstance(elementClass, size)
        if (size > 0) {
         Array.copy(reprVal, lo, result, 0, size)
        }
        result.asInstanceOf[Array[T]]
     }
     */
    val RddMem: RDD[Int] = sc.makeRDD(Seq[Int](1, 2, 3, 4),2)

    // TODO 落盘到指定目录 用来检查分区
    RddMem.saveAsTextFile("output")
    sc.stop()
  }
}
