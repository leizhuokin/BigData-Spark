package DataScoure

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object MySQL_read extends App {
  val conf =new SparkConf().setAppName("mysql").setMaster("local[*]")
  val sc=new SparkContext(conf)
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/spark?useSSL=false"
  val userName = "root"
  val password = "123456"
  //2. 从MySQL中读取数据
  val rdd = new JdbcRDD(sc,
    () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, password)
    },
    "select * from user where id >=? and id<=?;",
    1,
    10,
    1,
    r => (r.getInt(1), r.getString(2),r.getInt(3),r.getString(4))
  )
  println(rdd.count())
  rdd.foreach(println)
  sc.stop()
}
