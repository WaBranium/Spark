package old

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Agent1_2 {
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


    val clickRDD = lines1RDD.flatMap {
      line => {
        val datas = line.split("_")
        // 这里只要填充了剩余两个字段，就可以不用cogroup了
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        }
        if (datas(8) != "null") {
          val vals = datas(8).split(",")
          vals.map {
            action => {
              (action, (0, 1, 0))
            }
          }
        } else {
          Nil
        }
      }
    }


  }
}
