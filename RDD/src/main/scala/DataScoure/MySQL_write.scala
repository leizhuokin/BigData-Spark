package DataScoure

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object MySQL_write extends App {
  val conf = new SparkConf().setAppName("mysql").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/spark?useSSL=false"
  val userName = "root"
  val password = "123456"
  val rdd = sc.makeRDD(List(
    ("李四", 18, "111111111"),
    ("王五", 17, "222222222"),
    ("张彦", 18, "222222222"),
    ("张京", 17, "222222222"),
    ("金凤", 18, "222222222"),
    ("梦瑶", 18, "222222222"),
    ("王磊", 18, "222222222"),
    ("王斌", 17, "222222222"),
    ("徐林博", 17, "222222222"),
    ("刘杰", 17, "222222222")))
  rdd.foreach{
    case (name,age,phone) =>{
      val conn = DriverManager.getConnection(url,userName,password)
      val sql = "insert into user(name,age,phone) values(?,?,?)"
      val statement = conn.prepareStatement(sql)
      statement.setString(1,name)
      statement.setInt(2,age)
      statement.setString(3,phone)
      statement.executeUpdate()
      statement.close()
      conn.close()
    }
  }
  sc.stop()
}
