package cn.spark.study.advance

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit ={
    //create spark conf
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    //create language specified spark context
    val sc = new SparkContext(conf)
    val lines = sc.textFile("/Users/patricklo/Documents/big.txt", 1);
    val words = lines.flatMap(line => line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCounts = pairs.reduceByKey(_ + _)
    val countWords = wordCounts.map(t => (t._2,t._1))
    val sortedCountwords = countWords.sortByKey(false)
    val sortedWordcounts = sortedCountwords.map(t => (t._2,t._1))

    sortedWordcounts.foreach(wordCount => println(wordCount._1+" : "+wordCount._2))


  }


}
