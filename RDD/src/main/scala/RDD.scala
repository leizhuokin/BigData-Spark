import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SparkBasic extends App {
  val conf = new SparkConf().setAppName("ChickPoint").setMaster("local[*]")
  val sc = new SparkContext(conf)

  val rdd1 = sc.makeRDD(List(("abc",1),("ddd",2),("add",2),("edd",2)))
  val rdd2 = rdd1.partitionBy(new MyPartitioner(3))
  val result = rdd2.mapPartitionsWithIndex((index,its)=>Iterator(index+":"+its.mkString("-")))
  result.foreach(println)
  sc.stop()

}
class MyPartitioner(numPars :Int) extends Partitioner{
  override def numPartitions: Int = numPars

  override def getPartition(key: Any): Int = {
    key.toString.charAt(0)%numPartitions
  }
}
