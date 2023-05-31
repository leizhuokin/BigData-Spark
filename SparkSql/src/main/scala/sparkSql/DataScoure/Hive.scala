package sparkSql.DataScoure

import org.apache.spark.sql.SparkSession

object Hive {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession
      .builder()
      .appName("QUICK")
      .master("local[*]").getOrCreate()
    //读取数据
    val dataFrame = ss.read.json("in/user.json")
    //展示数据
    dataFrame.show()
    //关闭连接
    ss.stop()
  }
}
