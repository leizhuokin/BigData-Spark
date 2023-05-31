import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object window {
  def main(args: Array[String]): Unit = {
    //配置对象
    val conf = new SparkConf().setAppName("WindowTest").setMaster("local[3]")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(10))
    //重要：检查点目录的配置
    ssc.sparkContext.setCheckpointDir("./cp")
    //从Socket接收数据
    val lineDStream = ssc.socketTextStream("node01", 8888)
    val words = lineDStream.flatMap(_.split(" ")).map((_, 1))
    words.reduceByKey(_ + _).print()
    val window = words.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b, //reduce函数：将值进行叠加
      Seconds(20), //窗口的时间间隔，大小为3
      Seconds(9) //窗口滑动的时间间隔，步长为2
    )
    window.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
