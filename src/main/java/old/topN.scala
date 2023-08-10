package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * TODO 根据省份、广告进行统计，找出各省份排名前三的广告
 */
object topN {
  def main(args: Array[String]): Unit = {
    // TODO 首先获取数据
    // 加载配置为本地配置 设定任务名称为WordCount
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 设定配置到连接池中 建立连接 读取数据
    val sparkContext = new SparkContext(sparkConf)
    val sparkLines: RDD[String] = sparkContext.textFile("src/main/resources/agent.log")

    // TODO 首先将每行进行切割，提取关键字段：((省份,广告类型)、阅读量初始值(1))
    // 注意这里不要用flatMap 你是按照各行来进行操作统计 最后一行就是一个元素 又不是一行一个数组需要展平
    val sparkTruples = sparkLines.map(
      line => {
        val datas = line.split(" ")
        // 进行切割 保留一部分字段
        ((datas(2), datas(4)), 1)
      }
    )

    // TODO 根据省份和广告类型的聚合Key来叠加val;
    val sparkTruples2 = sparkTruples.reduceByKey(_ + _)


    // TODO 解开key限制 融合回去
    val sparkTruples_Res = sparkTruples2.map {
      case ((pro, adv), i) => {
        (pro, (adv, i))
      }
    }

    // TODO 对i进行排序
    val res = sparkTruples_Res.groupByKey().mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    res.collect().foreach(println)

  }
}
