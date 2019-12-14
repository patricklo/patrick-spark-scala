package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

// scala中有一些特殊的算子，也就是特殊的transformation操作，如groupByKey, reduceByKey,sortByKey等，其实只是针对特殊RDD=》 即包含key-value 对的RDD
// 而这种RDD中的元素，其实是scala中的Tuple2类型，也就是包含2个值的tuple
//
// 在scala中， 需要手动导入spark的相关隐式转换，->   SparkContext._
import org.apache.spark.SparkContext._

object LineCount {
  def main(args: Array[String]): Unit ={
    //create spark conf
    val conf = new SparkConf().setAppName("ParallelizeCollectionRDD").setMaster("local")
    //create language specified spark context
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/patricklo/Documents/test1.txt", 1);
    val scalaPairRDD = lines.map{ line => (line,1)}
    val lineCounts = scalaPairRDD.reduceByKey(_ + _)
    lineCounts.foreach(lineCount => println(lineCount))


  }

}
