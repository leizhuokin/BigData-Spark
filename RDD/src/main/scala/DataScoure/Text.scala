package DataScoure

import org.apache.spark.{SparkConf, SparkContext}

object Text extends App {
  val conf =new SparkConf().setAppName("text").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val rdd1 = sc.textFile("in/1.txt")
  val result = rdd1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
  result.saveAsTextFile("out/1.txt")
  sc.stop()
}
