package cn.edu.zut.spark.stream.util

import cn.edu.zut.spark.stream.entity.City
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.{Properties, Random}
import scala.collection.mutable.ArrayBuffer

object Mocker {
  def generateMockData(): Array[String] = {
    val array: ArrayBuffer[String] = ArrayBuffer[String]()
    val cityRandomOpt = RandomOptions[City](RanOpt(City(1, "北京", "华北"), 30),
      RanOpt(City(2, "上海", "华东"), 30),
      RanOpt(City(3, "广州", "华南"), 10),
      RanOpt(City(4, "深圳", "华南"), 20),
      RanOpt(City(5, "天津", "华北"), 10))
    val random = new Random()
    //摸拟实时数据：
    //timestomp province city userid adid
    for (1 <- 0 to 50) {
      val timestamp: Long = System.currentTimeMillis()
      val city = cityRandomOpt.getRandomopt
      val adId: Int = 1 + random.nextInt(6)
      val userId: Int = 1 + random.nextInt(6)
      //拼接实时数据
      array += timestamp + " " + city.area + " " + city.name + " " + userId + " " + adId
    }
    array.toArray
  }

  def createKafkaProducer(broker: String): KafkaProducer[String, String] = {
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    //根据配置创建Kafka生产者
    new KafkaProducer[String, String](prop)
  }

  def main(args: Array[String]): Unit = {
    //获取配置文件config.properties中的Kafka配置参数
    val config: Properties = PropertiesUtil.load("config.properties")
    val broker: String = config.getProperty("kafka.broker.list")
    val topic = "spark-stream-case"
    //创建Kafka生产者
    val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)
    while (true) {
      //随机产生实时数据并通过Kafka生产者发送到kafka集群中
      for (line <- generateMockData()) {
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))
        println(line)
      }
      Thread.sleep(2000)
    }
  }
}
