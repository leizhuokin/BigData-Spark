package Scoure

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * Spark有几种持久化级别
 * MEMORY_ONLY:使用未序列化的Java对象格式，将数据保存在内存中。如果内存不够存放所有的数据，则数据可能就不会进行持久化。
 * MEMORY_AND_DISK:使用未序列化的Java对象格式，优先尝试将数据保存在内存中。如果内存不够存放所有的数据，会将数据写入磁盘文件中
 * MEMORY_ONLY_SER:基本含义同MEMORY_ONLY。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个partition会被序列化成一个字节数组。
 * MEMORY_AND_DISK_SER:基本含义同MEMORY_AND_DISK。唯一的区别是，会将RDD中的数据进行序列化，RDD的每个partition会被序列化成一个字节数组。
 * DISK_ONLY:使用未序列化的Java对象格式，将数据全部写入磁盘文件中。
 * MEMORY_ONLY_2
 * MEMORY_AND_DISK_2
 * 加上后缀_2，代表的是将每个持久化的数据，都复制一份副本，并将副本保存到其他节点上。
 *
 * @param host
 * @param port
 */
class SparkStreamCustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  //启动的时候调用
  override def onStart(): Unit = {
    println("启动了")

    //创建一个socket
    val socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
    //创建一个变量去读取socket的输入流的数据
    var line = reader.readLine()
    while (!isStopped() && line != null) {
      //TODO 如果接收到了数据，就使用父类的中store方法进行保存
      store(line)
      //继续读取下一行数据
      line = reader.readLine()
    }

  }
  //停止的时候调用
  override def onStop(): Unit = {
    println("停止了")
  }
}

object Demo03_CustomReceiver extends App {
  //配置对象
  val conf = new SparkConf().setAppName("stream").setMaster("local[3]")
  //创建StreamingContext
  val ssc = new StreamingContext(conf, Seconds(5))
  //从Socket接收数据
  val lineDStream = ssc.receiverStream(new SparkStreamCustomReceiver("node01", 8888))
  //统计词频
  val result = lineDStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
  result.print()
  //启动
  ssc.start()
  ssc.awaitTermination()
}
