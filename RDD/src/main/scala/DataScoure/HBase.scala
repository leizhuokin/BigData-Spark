package DataScoure

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HBase extends App {
  val conf = new SparkConf().setAppName("HBase").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val hbaseConf = HBaseConfiguration.create()
  hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "node01:2181,node02:2181,node03:2181") //设置zookeeper集群，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
  hbaseConf.set(TableInputFormat.INPUT_TABLE, "user")

  //3. 从HBase读取数据形成RDD
  val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
    hbaseConf,
    classOf[TableInputFormat],//table
    classOf[ImmutableBytesWritable],//hbase table rowkey
    classOf[Result])//resultset
  //4. 对hbaseRDD进行处理
  rdd.foreach {
    case (_, res) =>
      for (cell <- res.rawCells()){
        print("RowKey: " + new String(CellUtil.cloneRow(cell)) + ", ");
        print("时间戳: " + cell.getTimestamp() + ", ");
        print("列名: " + new String(CellUtil.cloneQualifier(cell)) + ", ");
        println("值: " + new String(CellUtil.cloneValue(cell)));
      }
  }
  sc.stop()
}
