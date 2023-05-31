package cn.edu.zut.spark.stream.app

import cn.edu.zut.spark.stream.entity.AdLog
import cn.edu.zut.spark.stream.handler.BlackListHandler
import cn.edu.zut.spark.stream.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AdBlackUserApp {
  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf().setAppName("kafka2streaming").setMaster("local[3]")
    //创建StreamingContext对象
    val ssc = new StreamingContext(conf, Seconds(5))
    //3.读取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getDStream(ssc, "spark-stream-case")
    //4.将从 Kafka 读出的数据转换为样例类对象
    val adsLogDStream: DStream[AdLog] = kafkaDStream.map(record => {
      val value: String = record.value()
      val arr: Array[String] = value.split(" ")
      AdLog(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
    })
    //5.需求一：根据 MySQL 中的黑名单过滤当前数据集
    val filterAdsLogDStream: DStream[AdLog] = BlackListHandler.filterByBlackList(adsLogDStream)
    //6.需求一：将满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDStream, 100);
    //测试打印
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()
    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}

