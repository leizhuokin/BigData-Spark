import org.apache.spark.sql.SparkSession

object StreamingQuick {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder()
      .appName("wordcount")
      .master("local[*]")
      .getOrCreate()
    val line = spark.readStream
      .format("socket")
      .option("host", "node01")
      .option("port", "8888")
      .load()
    import spark.implicits._
//    val word = line.as[String].flatMap(_.split(" ")).groupBy("value").count()
    line.as[String].flatMap(_.split(" ")).createOrReplaceTempView("data")
    val word=spark.sql(
      """
        |select
        |value ,count(1) as count
        |from data group by value
        |""".stripMargin)
    // 三种模式：
    // 1 complete 所有内容都输出 聚合查询
    // 2 append   新增的行才输出 非聚合查询
    //        |select
    //        |*
    //        |from data
    // 3 update   更新的行才输出 不支持排序
    word.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }
}
