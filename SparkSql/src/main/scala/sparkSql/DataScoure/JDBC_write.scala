package sparkSql.DataScoure

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.Properties

object JDBC_write extends App {
  case class User(id: Long, name: String, age: Long, phone: String)
  val spark = SparkSession
    .builder()
    .appName("Quick")
    .master("local[*]").getOrCreate()
  val rdd: RDD[User] = spark.sparkContext.makeRDD(List(
    User(11, "张炎峰", 25, "12345678901"),
    User(12, "李文", 23, "12345678902")
  ))

  import spark.implicits._

  val ds: Dataset[User] = rdd.toDS
  //方式 1：通用的方式 format 指定写出类型
  ds.write
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark")
    .option("user", "root")
    .option("password", "123456")
    .option("dbtable", "user")
    .mode(SaveMode.Append)
    .save()
  //方式 2：通过 jdbc 方法
  val props: Properties = new Properties()
  props.setProperty("user", "root")
  props.setProperty("password", "123456")
  ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark", "user", props)
  //释放资源
  spark.stop()
}
