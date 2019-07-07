package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount");
    val sc = new SparkContext(conf)
    val lines = sc.textFile("http://spark1:9000/test.txt", 1);
    val words = lines.flatMap(line => line.split("  "));
    val pairs = words.map(word => (word, 1));
    val wordCounts = pairs.reduceByKey(_ + _);
    wordCounts.foreach(wordCount => println(wordCount._1 + " appears " + wordCount._2 + " times"));
  }

}
