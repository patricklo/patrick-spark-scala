package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}


/**
  * 1. reduce
  * 2. collect
  * 3. count
  * 4. take
  * 5. saveAsTextfile
  * 6. countByKey
  * 7. foreach
  */
object ActionFullExample {
  def main(args: Array[String]): Unit ={
    reduce()
  }
  def reduce(): Unit={
    //创建`spark conf
    val sparkConf = new SparkConf().setAppName("ActionFullExample-Reduce").setMaster("local")
    //创建context
    val scalaSparkContxt = new SparkContext(sparkConf)
    //构建集合
    var numbers = Array(1,2,3,4,5,6,7,8,9)
    //并行化集合， 创建初始RDD
    var numbersRDD = scalaSparkContxt.parallelize(numbers)
    //val totalResult = numbersRDD.reduce((v1,v2) => v1+v2)
    val totalResult = numbersRDD.reduce(_ + _)
    println("scala" + totalResult)
  }
}
