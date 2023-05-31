
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

import java.sql.Timestamp

object WindowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("windowTest")
      .getOrCreate()
    val line = spark.readStream
      .format("socket")
      .option("host", "node01")
      .option("port", "8888")
      .option("includeTimestamp", "true")
      .load()
    import spark.implicits._
    val words = line.as[(String,Timestamp)].flatMap(e=>{
      e._1.split(" ").map((_,e._2))
    }).toDF("word","dt")
    val count = words.groupBy(
      window($"dt", "10 seconds", "5 seconds"),$"word"
    ).count().orderBy("window")
    count.writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
