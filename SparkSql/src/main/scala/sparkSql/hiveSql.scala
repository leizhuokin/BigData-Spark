package sparkSql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object hiveSql extends App{
  val spark = SparkSession
    .builder()
    .appName("SQL Hive")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "hdfs://192.168.100.101:9000/hive/warehouse/")
    .enableHiveSupport()
    .getOrCreate()
  spark.udf.register("cityRemark",functions.udaf(cityRemarkUDAF))
  spark.sql(
    """
      |select
      |city.*,
      |product.product_name,
      |click_product_id
      |from user_visit_action as user
      |join product_info as product on user.click_product_id=product.product_id
      |join city_info as city on user.city_id=city.city_id
      |""".stripMargin).createOrReplaceTempView("t1")
  spark.sql(
    """
      |select
      |area,product_name,
      |count(area) as clickCount,
      |cityRemark(city_name) as city_remark
      |from t1
      |group by area,product_name
      |""".stripMargin).createOrReplaceTempView("t2")
  spark.sql(
    """
      |select
      |*,
      |rank() over(partition by area order by clickCount desc) as rank
      |from  t2
      |""".stripMargin).createOrReplaceTempView("t3")
  spark.sql("select * from t3 where rank <=3").show()
  spark.stop()

}
case class BufferData(var total:Long,var city:mutable.Map[String,Long])
object cityRemarkUDAF extends Aggregator[String,BufferData,String]{
  override def zero: BufferData = {
    BufferData(0,mutable.Map[String,Long]())
  }

  override def reduce(b: BufferData, a: String): BufferData = {
    b.total+=1
    val count=b.city.getOrElse(a,0L)+1
    b.city.update(a,count)
    b
  }

  override def merge(b1: BufferData, b2: BufferData): BufferData = {
    b1.total+=b2.total
    val map1=b1.city
    val map2=b2.city
    map2.foreach{
      case (city, cnt) => {
        val count = map1.getOrElse(city, 0L) + cnt
        map1.update(city, count)
      }
    }
    b1.city = map1
    b1
  }

  override def finish(reduction: BufferData): String = {
    val remark = ListBuffer[String]()
    val total = reduction.total
    val city = reduction.city
    //降序
    var cityTotalList = city.toList.sortWith(
      (left, right) => {
        left._2 > right._2
      }
    ).take(2)

    val hasMore = city.size > 2
    var sum = 0L
    cityTotalList.foreach {
      case (city, cnt) => {
        val r = cnt * 100 / total
        remark.append(s"${city} ${r}%")
        sum += r
      }
    }
    if (hasMore) {
      remark.append(s"其他${100 - sum}%")
    }

    remark.mkString(",")
  }

  override def bufferEncoder: Encoder[BufferData] =Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}