package DataScoure

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Json extends App {
  val conf =new SparkConf().setAppName("json").setMaster("local[*]")
  val sc =new SparkContext(conf)
  val rdd =sc.textFile("in/user.json")
  val jsonRDD=rdd.map(JSON.parseFull)
  jsonRDD.foreach(println)
  sc.stop()
}
