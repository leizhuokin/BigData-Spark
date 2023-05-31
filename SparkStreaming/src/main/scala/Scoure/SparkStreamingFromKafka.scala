package Scoure

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingFromKafka {

  def main(args: Array[String]): Unit = {
    //创建配置对象
    val conf = new SparkConf().setAppName("kafka2streaming").setMaster("local[3]")
    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(5))

    //获取数据的主题
    val fromTopic = "spark"
    val brokers = "node01:9092,node02:9092,node03:9092"
    //创建kafka连接参数
    val kafkaParams = Map[String,Object](
      //集群地址
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      //Key与VALUE的反序列化类型
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->classOf[StringDeserializer],
      //创建消费者组
      ConsumerConfig.GROUP_ID_CONFIG->"StreamingKafka",
      //自动移动到最新的偏移量
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG->"latest",
      //启用自动提交，将会由Kafka来维护offset【默认为true】
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG->"true"
    )
    //获取DStream
    val dStream = KafkaUtils.createDirectStream(
      ssc,//SparkStreaming操作对象
      LocationStrategies.PreferConsistent,//数据读取之后如何分布在各个分区上
      /*
      PreferBrokers：仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在kafka broker的机器上
      PreferConsistent：大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
      PreferFixed：如果你的负载不均衡，可以通过这种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配
       */
      ConsumerStrategies.Subscribe[String,String](Array(fromTopic),kafkaParams)
    )
    //处理DStream的数据——业务逻辑
    //对数据进行拼接操作
    val result = dStream.map(x=>x.value()+"--------hahahha")
    //结果输出
    result.print

    //启动
    ssc.start()
    ssc.awaitTermination()
  }
}
