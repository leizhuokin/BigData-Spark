import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

case class ScoreData(name: String,course: String,score:Int,year:String)
object BasicTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    val line = spark.readStream
      .format("socket")
      .option("host", "node01")
      .option("port", "8888")
      .load()
    import spark.implicits._
    val ds:Dataset[ScoreData] = line.map(e => {
      val strs = e.get(0).toString.split(" ")
      ScoreData(strs(0), strs(1), strs(2).toInt, strs(3))
    })

    // val result = ds.filter(_.score >= 60).select("name","course","score","year")
    //val result = ds.groupBy("course").count()
    val result = ds.groupBy("course")
      .agg(avg("score"),
        count("course"),
        sum("score"))
    result.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
