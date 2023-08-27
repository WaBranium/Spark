package RddOperator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 *  TODO 多种实现wordcount的方法
 */
object WordCountsDemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)

    // TODO 准备数据
    val Rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello Spark"))

    // TODO 利用行动算子 groupBy
    Rdd.flatMap(_.split(" "))
      // 行动算子 表明根据单词本身进行聚合 成(word,iter[T])
      .groupBy(data => data)
      .map{ // 也可以直接使用mapValues
        case (key,iter) => {
          (key,iter.size)
        }
      }
      .foreach(println)

    println("==========================")
    // TODO 利用行动算子两个countBy
    Rdd.flatMap(_.split(" "))
      // 基于数据本身进行计数
      .countByValue()
      .foreach(println)

    println("==========================")
    // TODO 利用行动算子两个countBy
    Rdd.flatMap(_.split(" "))
      .map((_,1))
      // 基于数据本身进行计数
      .countByKey()
      .foreach(println)

    println("==========================")
    // TODO 利用转换算子 groupByKey
    Rdd.flatMap(_.split(" "))
      .map((_, 1))
      .groupByKey()
      .map{
        case (key,iter) => (key,iter.size)
      }
      .foreach(println)

    println("==========================")
    // TODO 利用转换算子 四大ByKey
    Rdd.flatMap(_.split(" "))
      .map( (_,1) )
      .reduceByKey(_+_)
      .collect()
      .foreach(println)

    println("==========================")
    // TODO 利用转换算子 四大ByKey
    Rdd.flatMap(_.split(" "))
      .map((_, 1))
      .foldByKey(0)(_ + _)
      .collect()
      .foreach(println)

    println("==========================")
    // TODO 利用转换算子 四大ByKey
    Rdd.flatMap(_.split(" "))
      .map((_, 1))
      .aggregateByKey(0)(_ + _ , _+_)
      .collect()
      .foreach(println)

    println("==========================")
    // TODO 利用转换算子 四大ByKey
    Rdd.flatMap(_.split(" "))
      .map((_, 1))
      .combineByKey(data => data , (x:Int,y)=>x+y , (x:Int,y:Int)=>x+y)
      .collect()
      .foreach(println)

    println("==========================")
    // TODO 利用reduce agg fold来进行 主要是将之前的队列变为map 再进行mao里面元素的插入与更新
    //  上面这哥仨主要是针对最外层List的数据元素间进行操作 那么将数据元素转换为一个个map 让map直接互相操作 数据不断更新 模拟之前的叠加即可
    val maps: RDD[mutable.Map[String, Long]] = Rdd.flatMap(_.split(" "))
      .map(
        word => {
          mutable.Map[String, Long]((word, 1))
        }
      )

    maps.reduce(
      (map1, map2) => {
        // map1代表初始map map2代表放进来的map
        map2.foreach {
          case (word, count) => {
            val num: Long = map1.getOrElse(word, 0L) + count
            map1.update(word, num)
          }
        }
        map1
      }
    ).foreach(println)

    println("====================================")
    maps.aggregate(mutable.Map[String,Long]())(
        (map1,map2)=>{
            // map1代表初始map map2代表放进来的map
            map2.foreach{
              case (word,count) =>{
                  val num: Long = map1.getOrElse(word,0L)+count
                  map1.update(word,num)
              }
            }
           map1
        },
        (map1, map2) => {
          // map1代表初始map map2代表放进来的map
          map2.foreach {
            case (word, count) => {
              val num: Long = map1.getOrElse(word, 0L) + count
              map1.update(word, num)
            }
          }
          map1
        }
      )
      .foreach(println)


    sc.stop()
  }

}
