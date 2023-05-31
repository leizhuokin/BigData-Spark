package Scoure

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Demo02_RDD {
  val conf = new SparkConf().setAppName("StreamRDD").setMaster("local[3]")
  val ssc = new StreamingContext(conf, Seconds(2))

  //创建RDD队列
  val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
  //创建DStream
  val dStream = ssc.queueStream(rddQueue)
  //处理DStream的数据——业务逻辑
  //将数据叠加起来，计算总和
  val result = dStream.map(x => (1, x)).reduceByKey(_ + _)
  //结果输出
  result.print

  //启动
  ssc.start()
  //生产数据
  for (i <- 1 to 1000) {
    val r = ssc.sparkContext.makeRDD(1 to 100)
    r.collect()
    rddQueue += r
    Thread.sleep(3000)
  }
  ssc.awaitTermination()
}
