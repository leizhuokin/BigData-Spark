package cn.edu.zut.spark.stream.handler

import cn.edu.zut.spark.stream.entity.AdLog
import cn.edu.zut.spark.stream.util.DBUtil
import org.apache.spark.streaming.dstream.DStream

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

object BlackListHandler {
  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 添加至黑名单中，如果符合条件的话
   * @param filterAdLogDSteam 每一个批次的DStream
   * @param threshold 黑名单点击次数的阈值，超过threshold次的广告点击，视为恶意点击
   */
  def addBlackList(filterAdLogDSteam: DStream[AdLog], threshold:Int): Unit = {
    //统计当前批次中单日每个用户点击每个广告的总次数
    //1.将数据接转换结构 ads_log=>((date,user,ad),1)
    val dateUserAdToOne: DStream[((String, String, String), Long)] =
    filterAdLogDSteam.map(adLog => {
      //a.将时间戳转换为日期字符串
      val date: String = sdf.format(new Date(adLog.timestamp))
      //b.返回值
      ((date, adLog.userId, adLog.adId), 1L)
    })
    //2.统计单日每个用户点击每个广告的总次数
    //((date, user, adId), 1)=> ((date, user, adId), count)
    val dateUserAdToCount: DStream[((String, String, String), Long)] = dateUserAdToOne.reduceByKey(_ + _)
    //3. 将结果写入到MySQL
    dateUserAdToCount.foreachRDD(rdd => {
      rdd.foreachPartition(item => {
        val conn: Connection = DBUtil.getConnection
        item.foreach {
          case ((dt, user, ad), count) => DBUtil.executeUpdate(
            conn,
            """
              |insert into user_ad_count (dt,user_id,ad_id,count)
              |values (?,?,?,?)
              |on duplicate key
              |update count=count+?
            """.stripMargin, Array(dt, user, ad, count, count))
            val result = DBUtil.queryOneMap(conn, "select count from user_ad_count where dt =? and user_id =? and ad_id =?", Array(dt, user, ad))
            val adCount = result("count").toString.toInt
            if (adCount >= threshold) {
              DBUtil.executeUpdate(
                conn,
                "insert into black_user (user_id) values (?) on duplicate key update user_id =?",
                Array(user, user)
              )
            }
        }
        conn.close()
      })
    })
  }

  /**
   * 过滤黑名单
   * @param adLogDStream  每一个批次的DStream
   * @return
   */
  def filterByBlackList(adLogDStream: DStream[AdLog]): DStream[AdLog] = {
    adLogDStream.transform(rdd => {
      rdd.filter(adLog => {
        val conn: Connection = DBUtil.getConnection
        val bool: Boolean = DBUtil.isExist(
          conn,
          "select * from black_user where user_id =? ",
          Array(adLog.userId))
        conn.close()
        !bool
      })
    })
  }
}

