package HelloWorld

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author BiHan
 * 核心思想：按行读取后，对文件流做扁平化处理，类似大爆炸的效果得到的是一堆单词的集合；
 *         然后将集合根据自身进行聚合，形成（key,values）的Tuple；
 *         最后获取values的长度，即可转变
 */
object WordCount1 {

  def main(args: Array[String]): Unit = {

    // TODO 建立Spark链接
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context = new SparkContext(sparkConf)

    // TODO 获取元数据
    val lines: RDD[String] = context.textFile("{datas/*}")

    // TODO 业务逻辑处理——WordCount
    // 把每一行字符串拆成一个一个单词 并收集在一个集合里面（大爆炸）
    // 简略写法
    //    val value1 = lines.flatMap(_.split(" "))
    var words: RDD[String] = lines.flatMap(
      // 这里面写针对每一行字符串操作的抽象方法
      (line: String) => {
        // 写针对单位行的具体处理逻辑 并且最后一行默认为返回的Unit
        line.split(" ")
      }
    )
    // 接下来进行聚合
    var values: RDD[(String, Iterable[String])] = words.groupBy(
      (word: String) => {
        word
      }
    )

    // 整改Tuple类型为 (string,list.size)
    val wordToCount: RDD[(String, Int)] = values.map {
      case (str, strings) => {
        Tuple2(str, strings.size)
      }
    }

    // TODO 输出并关闭
    wordToCount.collect().foreach(println)
    context.stop()
  }

}
