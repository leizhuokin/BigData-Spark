package sparkSql.DataScoure

import org.apache.spark.sql.SparkSession
import java.util.Properties

object JDBC_read extends App {
  val spark = SparkSession
    .builder()
    .appName("Quick")
    .master("local[*]").getOrCreate()
  //方式 1：通用的 load 方法读取
  spark.read.format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/spark")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("user", "root")
    .option("password", "123456")
    .option("dbtable", "user")
    .load().show
  //方式 2:通用的 load 方法读取 参数另一种形式
  spark.read
    .format("jdbc")
    .options(Map(
      "url" -> "jdbc:mysql://localhost:3306/spark?user=root&password=123456",
      "dbtable" -> "user",
      "driver" -> "com.mysql.cj.jdbc.Driver"
    )).load().show

  //方式 3:使用 jdbc 方法读取
  val props: Properties = new Properties()
  props.setProperty("user", "root")
  props.setProperty("password", "123456")
  val df = spark.read.jdbc("jdbc:mysql://localhost:3306/spark", "user", props)
  df.show
  //释放资源
  spark.stop()
}
