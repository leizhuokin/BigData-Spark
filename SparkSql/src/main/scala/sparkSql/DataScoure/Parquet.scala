package sparkSql.DataScoure


import org.apache.spark.sql.SparkSession

object Parquet extends App {
  val spark = SparkSession
    .builder()
    .appName("QUICK")
    .master("local[*]").getOrCreate()
  val df=spark.read.json("data/score.json")
  df.describe("grade").show()
  df.filter("subject = ' math' or grade>60").show(6)
  df.groupBy("name","subject").avg("grade").show()
  df.groupBy("name","Chinese","math").avg("grade").show()
}
