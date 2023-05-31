import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingTelnet extends App {
  val conf = new SparkConf().setAppName("stream").setMaster("local[3]")
  val ssc = new StreamingContext(conf,Seconds(10))
  val lineDStream = ssc.socketTextStream("node01",8888)
  //统计词频
  val result = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
  result.print()
  ssc.start()
  //采集器一直运行不结束
  ssc.awaitTermination()
}
