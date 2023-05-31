package sparkSql

import org.apache.spark.sql.SparkSession

object hiveSql2 extends App{
  val spark = SparkSession
    .builder()
    .appName("SQL Hive")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "hdfs://192.168.100.101:9000/hive/warehouse/")
    .enableHiveSupport()
    .getOrCreate()
  spark.sql("use user")
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
      |area,product_name,count(area) as clickCount
      |from t1
      |group by area,product_name
      |""".stripMargin).show()
  spark.stop()
}
