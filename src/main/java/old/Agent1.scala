package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 一次性统计每个品类点击的次数，下单的次数和支付的次数：（品类，（点击总数，下单总数，支付总数））
 * 然后 使用累加器的方式聚合数据
 * 其中各字段：品类6 点击数认为是1然后reduceByKey
 * 下单总数，拆开后合成map即可
 * 支付总数同理
 */
object Agent1 {
  def main(args: Array[String]): Unit = {
    // TODO 首先获取数据
    // 加载配置为本地配置 设定任务名称为WordCount
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 设定配置到连接池中 建立连接 读取数据
    val sparkContext = new SparkContext(sparkConf)
    val LinesRDD: RDD[String] = sparkContext.textFile("src/main/resources/user_visit_action.txt")

    // TODO 分别统计每个品类点击的次数，下单的次数和支付的次数
    val lines1RDD = LinesRDD.filter {
      action => {
        val strings = action.split("_")
        strings(6) != "-1"
      }
    }

    val clickRDD = lines1RDD.map {
      line => {
        val datas = line.split("_")
        // 这里只要填充了剩余两个字段，就可以不用cogroup了
        (datas(6), (1, 0, 0))
      }
    }
    val clickRes = clickRDD.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, 0, 0)
      }
    }

    val lines2RDD = LinesRDD.filter {
      action => {
        val strings = action.split("_")
        strings(8) != "null"
      }
    }
    val categoryRDD = lines2RDD.flatMap(
      line => {
        val categoryLine = line.split("_")(8)
        val categoryList = categoryLine.split(",")
        categoryList.map {
          item => {
            (item, (0, 1, 0))
          }
        }
      }
    ).reduceByKey {
      (t1, t2) => {
        (0, t1._2 + t2._2, 0)
      }
    }

    categoryRDD.collect().foreach(println)

  }
}
