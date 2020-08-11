package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}


object C5BrocastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Broadcast Variable").setMaster("local")
    val sc = new SparkContext(conf)
    val factor =3;
    val factorBroadcast = sc.broadcast(factor);
    val numberArray = Array(1,2,3,4,5)
    val numbers = sc.parallelize(numberArray,1)
    val multipleNumbers = numbers.map{num => num * factorBroadcast.value}

    multipleNumbers.foreach(num => println(num))
  }


}
