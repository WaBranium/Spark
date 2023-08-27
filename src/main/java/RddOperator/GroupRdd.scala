package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object GroupRdd {

  def main(args: Array[String]): Unit = {
    // TODO 建立运行环境
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupBy")
    val sc = new SparkContext(conf)

    // TODO 建立算子
    val Rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 方法里面填写按照什么条件去进行聚合 （shuffle）
    val value: RDD[(Int, Iterable[Int])] = Rdd.groupBy( _%2 )

    // TODO 输出 关闭
    value.collect().foreach(println)


    // TODO 案例DEMO : 聚合访问时间段
    sc.textFile("{datas/apache.log}")
    // 先做转换，提取出元组类型
   .map(
      elem => {
        val time: String = elem.split(" ")(3)
        val dateFormat = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")
        val dateStr: Date = dateFormat.parse(time)
        // 提取出小时
        val hour: String = new SimpleDateFormat("HH").format(dateStr)
        // 整合出元组
        hour
      }
    ).groupBy(
      data => {
        data
      }
      )
      .map{
        case (hour, iter) => {
          (hour,iter.size)
        }
      }
      .collect()
      .foreach(println)




    sc.stop()
  }

}
