package RddOperator
import org.apache.spark.{SparkConf, SparkContext}

object AggByRdd {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // 获取 RDD
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2),("b",1), ("a", 3), ("a",4),("b",2) ),2)

    // TODO 使用这个方法的时候 会先在分区里针对相同的Key进行聚合（key,iter[T]）然后再根据key进行分区内与分区间计算
    //  当然 他也是可以预聚合的
    rdd.aggregateByKey(0)( // 第一个是初始值，用于在分区内进行计算判断时声明类型与进行实际比较
      //  这里是传递分区内计算规则，与分区间元素计算规则的，先内后外
      (x, y) => math.max(x,y),
      _+_
    ).collect().foreach(println)

    println("=====================")
    // TODO 求平均值
    rdd.aggregateByKey((0,0))(
      // 初始状态下 t1是上面的（0,0） t2是iter[T]里面的元素
      (t1,v)=> (t1._1+v,t1._2+1),
      // 分区间就是各个(0,0)经过累加后得到的区内最终结果出来继续计算
      (t1,t2) => (t1._1+t2._1,t2._2+t2._2)
    ).mapValues{
      case  (num, cnt) => {
        (num.toDouble/cnt.toDouble)
      }
    }.collect()
      .foreach(println)

    sc.stop()

  }

}
