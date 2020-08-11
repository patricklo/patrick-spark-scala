package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object C6AccumulatorVar {
  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setAppName("AccumulatorVar").setMaster("local")
    val sc = new SparkContext(conf)
    val accumulatorFactor = sc.accumulator(0)

    val numberArr = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArr,1)
    numbers.foreach(num => accumulatorFactor += num)


  println(accumulatorFactor)

  }
}
