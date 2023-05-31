package cn.edu.zut.spark.stream.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

object KafkaUtil {
  //1.创建配置信息对象
  private val properties: Properties = PropertiesUtil.load("config.properties")
  //2.用于初始化链接到集群的地址
  val brokers: String = properties.getProperty("kafka.broker.list")
  //3.kafka 消费者配置
  val kafkaParams: Map[String, Object] = Map[String, Object](
    //集群地址
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
    //Key与VALUE的反序列化类型
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    //创建消费者组
    ConsumerConfig.GROUP_ID_CONFIG -> "spark-stream-case-group",
    //自动移动到最新的偏移量
    //earliest:表示如果有offset记录从offset记录开始消费,如果没有从最早的消息开始消费
    //latest:表示如果有offset记录从offset记录开始消费,如果没有从最后/最新的消息开始消费
    //none:表示如果有offset记录从offset记录开始消费,如果没有就报错
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    //启用自动提交，将会由Kafka来维护offset【默认为true】
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )
  def getDStream(ssc: StreamingContext, topic:String):InputDStream[ConsumerRecord[String, String]] = {
    //获取DStream
    KafkaUtils.createDirectStream(
      ssc, //SparkStreaming操作对象
      LocationStrategies.PreferConsistent, //数据读取之后如何分布在各个分区上
      /*
      PreferBrokers：仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在kafka broker的机器上
      PreferConsistent：大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
      PreferFixed：如果你的负载不均衡，可以通过这种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配
       */
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParams)
    )
  }
}
