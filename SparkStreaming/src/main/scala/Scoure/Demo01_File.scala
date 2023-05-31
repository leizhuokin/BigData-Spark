package Scoure

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo01_File extends App{
  //1.创建Spark配置对象
  val conf = new SparkConf().setMaster("local[*]").setAppName("file")
  //sc ss/spark ssc
  //2. 创建 StreamingContext 对象
  val ssc = new StreamingContext(conf, Seconds(5))

  //3. 创建DStream监控文件夹
//  val ds = ssc.textFileStream("hdfs://node01:9000/stream/in")
  val ds = ssc.socketTextStream("node01",8888)
  //stream dstream ds
  //4. 做词频统计
  val result = ds.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)

  //5.打印
  result.print()

  //6.启动SparkStreamingContext
  ssc.start()
  ssc.awaitTermination()
}
