package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 常用transformation 案例
  * 常用transformation 介绍
  * 1. map  将RDD中的每个元素传入自定义函数，获取一个新的元素，然后用新的元素组成新的RDD并返回，可进行下一步处理
  * 2. filter  对RDD的元素进行判断，如返回TRUE则保留，反之则剔除
  * 3. flatMap 与map类似，但每个元素都可以返回一个或多个新元素
  * 4. groupByKey
  * reduceByKey 对每个key对应的value进行reduce操作
  * 5. sortByKey
  * 6. join 对2个包含<Key,value>对的RDD进行join操作，每个key join上的新pair，都会传入到自定义函数进行处理
  * 7. cogroup 同join,但每个key上的iterable<value>都会传入自定义函数进行处理
  */
object TransformationFullExampleScala {
  /**
    * 1. map    将集合每个元素乘以2
    * 2. filter 过滤出集合中的偶数
    * 3. flatMap 将行拆分成单词
    * 4. groupByKey 将每个班级成绩进行分组
    *    reduceByKey 统计每个班级的总分
    * 5. sortByKey 将学生分数进行排序
    * 6. join 打印每个学生的成绩
    * 7. cogroup 打印每个学生的成绩
    */
  def main(args: Array[String]): Unit ={
    //map()
    //filter()
    //flatMap()
    //groupByKey()
    //reduceByKey()
    //sortByKey()
    cogroup()
  }


  /**
    * 1. map    将集合每个元素乘以2
    */
  def map():Unit={
    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")
    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val numbers = Array(1,2,3,4,5);
   //并行化集合，创建初始RDD
    val numberRDD = scalaSparkContext.parallelize(numbers,1);

    //使用map鼻子，将集合中每个元素都乘以2
    //map算子，是对任何类型的RDD，都可以调用
    //在java中，map算子接收的参数是function函数
    //创建的function对象 ，一定会让你设置第二个泛型参数，即返回的新元素类型
    val multiplyNumberRDD = numberRDD.map(num => num*2)
    //打印新的RDD
    multiplyNumberRDD.foreach(num => println(num))
  }
  /**
    * 1. filter
    */
  def filter():Unit={
    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")
    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val numbers = Array(1,2,3,4,5);
    //并行化集合，创建初始RDD
    val numberRDD = scalaSparkContext.parallelize(numbers,1);

    val filterNumRDD = numberRDD.filter(num => num%2==0)
    //打印新的RDD
    filterNumRDD.foreach(num => println(num))
  }

  /**
    * flatMap
    */

  def flatMap(): Unit ={
    //创建sparkconf
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")
    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val lines = Array("Hello you","hello me");
    //并行化集合，创建初始RDD
    val linesRDD = scalaSparkContext.parallelize(lines,1);

    val flatMapRDD = linesRDD.flatMap(line => line.split(" "))
    //打印新的RDD
    flatMapRDD.foreach(word => println(word))
  }

  /**
    * groupByKey
    */
  def groupByKey():Unit={
    //创建sparkconfig
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")

    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val scores = Array(Tuple2("class1",50),Tuple2("class2",60),Tuple2("class1",100),Tuple2("class2",90))
    //并行化集合，创建初始RDD
    val scoresRDD = scalaSparkContext.parallelize(scores)

    val groupedScoresRDD = scoresRDD.groupByKey()

    groupedScoresRDD.foreach(t => println("scala: "+t._1+":"+t._2))
  }

  /**
    * reduceByKey
    */
  def reduceByKey():Unit={
    //创建sparkconfig
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")

    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val scores = Array(Tuple2("class1",50),Tuple2("class2",60),Tuple2("class1",100),Tuple2("class2",90))
    //并行化集合，创建初始RDD
    val scoresRDD = scalaSparkContext.parallelize(scores)

    val totalScoresRDD = scoresRDD.reduceByKey(_+_)

    totalScoresRDD.foreach(t => println("scala: "+t._1+":"+t._2))
  }

  def sortByKey():Unit={
    //创建sparkconfig
    val sparkConf = new SparkConf().setAppName("TransformationFullExampleScala").setMaster("local")

    //创建context
    val scalaSparkContext = new SparkContext(sparkConf)
    //构建集合
    val scores = Array(Tuple2(50,"Leo"),Tuple2(60,"Patrick"),Tuple2(100,"Jack"),Tuple2(90,"Lix"))
    //并行化集合，创建初始RDD
    val scoresRDD = scalaSparkContext.parallelize(scores)

    val sortedScoresRDD = scoresRDD.sortByKey(false)

    sortedScoresRDD.foreach(t => println("scala: "+t._1+":"+t._2))
  }

  def cogroup(): Unit ={
    //创建`spark conf
    val sparkConf = new SparkConf().setAppName("Cogroup").setMaster("local")
    //创建context
    val scalaSparkContxt = new SparkContext(sparkConf)
    //构建集合
    var students = Array(Tuple2(1,"leo"), Tuple2(2,"Dan"), Tuple2(3,"Ace"), Tuple2(4,"Lix"))
    var scores = Array(Tuple2(1,60), Tuple2(2,60), Tuple2(3,70), Tuple2(4,90), Tuple2(1,100))

    //并行化集合， 创建初始RDD
    var studentsRDD = scalaSparkContxt.parallelize(students)
    var scoresRDD = scalaSparkContxt.parallelize(scores)

    val totalResult = studentsRDD.cogroup(scoresRDD)

    totalResult.foreach(result => println("scala: " + result._1+":"+result._2))
  }
}
