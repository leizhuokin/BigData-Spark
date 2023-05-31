package cn.edu.zut.spark.stream.util

import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DBUtil {
  //初始化连接池
  var dataSource: DataSource = init()
  //初始化连接池方法
  def init(): DataSource = {
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")
    properties.setProperty("driverClassName", config.getProperty("db.driver.class.name"))
    properties.setProperty("url", config.getProperty("db.url"))
    properties.setProperty("username", config.getProperty("db.user"))
    properties.setProperty("password", config.getProperty("db.password"))
    properties.setProperty("maxActive", config.getProperty("db.datasource.size"))
    DruidDataSourceFactory.createDataSource(properties)
  }
  //获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  /**
   * 执行 SQL 语句,单条数据插入
   * @param connection 数据库链接
   * @param sql SQL语句
   * @param params 参数
   * @return 数据库中受影响的行数
   */
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var result = 0
    var ps: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          ps.setObject(i + 1, params(i))
        }
      }
      result = ps.executeUpdate()
      connection.commit()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
   * 执行 SQL 语句,批量数据插入
   * @param connection 数据库链接
   * @param sql SQl语句
   * @param paramsList 参数列表
   * @return
   */
  def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {
    var result: Array[Int] = null
    var ps: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      ps = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            ps.setObject(i + 1, params(i))
          }
          ps.addBatch()
        }
      }
      result = ps.executeBatch()
      connection.commit()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
   * 判断一条数据是否存在
   * @param connection 数据库链接
   * @param sql SQL语句
   * @param params 参数列表
   * @return
   */
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      flag = ps.executeQuery().next()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  /**
   * 获取 MySQL 的多条数据
   *
   * @param connection 数据库链接对象
   * @param sql        SQL语句
   * @param params     参数
   * @return
   */
  def query(connection: Connection, sql: String, params: Array[Any]): List[List[AnyRef]] = {
    val result = ListBuffer[List[AnyRef]]()
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      val rs: ResultSet = ps.executeQuery()
      val metaData = ps.getMetaData
      val columnCount = metaData.getColumnCount
      while (rs.next()) {
        val tmp = ListBuffer[AnyRef]()
        for (i <- 1 to columnCount) {
          tmp.append(rs.getObject(i))
        }
        result.append(tmp.toList)
      }
      rs.close()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toList
  }

  /**
   * 获取 MySQL 的多条数据
   *
   * @param connection 数据库链接对象
   * @param sql        SQL语句
   * @param params     参数
   * @return
   */
  def queryMap(connection: Connection, sql: String, params: Array[Any]): List[Map[String,AnyRef]] = {
    val result = ListBuffer[Map[String,AnyRef]]()
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      val rs: ResultSet = ps.executeQuery()
      val metaData = ps.getMetaData
      val columnCount = metaData.getColumnCount
      while (rs.next()) {
        val tmp = mutable.Map[String,AnyRef]()
        for (i <- 1 to columnCount) {
          tmp.put(metaData.getColumnName(i), rs.getObject(i))
        }
        result.append(tmp.toMap)
      }
      rs.close()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toList
  }

  /**
   * 获取 MySQL 的多条数据
   *
   * @param connection 数据库链接对象
   * @param sql        SQL语句
   * @param params     参数
   * @return
   */
  def queryOne(connection: Connection, sql: String, params: Array[Any]): List[AnyRef] = {
    val result = ListBuffer[AnyRef]()
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      val rs: ResultSet = ps.executeQuery()
      val metaData = ps.getMetaData
      val columnCount = metaData.getColumnCount
      if (rs.next()) {
        for (i <- 1 to columnCount) {
          result.append(rs.getObject(i))
        }
      }
      rs.close()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toList
  }

  /**
   * 获取 MySQL 的多条数据
   *
   * @param connection 数据库链接对象
   * @param sql        SQL语句
   * @param params     参数
   * @return
   */
  def queryOneMap(connection: Connection, sql: String, params: Array[Any]): Map[String,AnyRef] = {
    val result = mutable.Map[String,AnyRef]()
    var ps: PreparedStatement = null
    try {
      ps = connection.prepareStatement(sql)
      for (i <- params.indices) {
        ps.setObject(i + 1, params(i))
      }
      val rs: ResultSet = ps.executeQuery()
      val metaData = ps.getMetaData
      val columnCount = metaData.getColumnCount
      if (rs.next()) {
        for (i <- 1 to columnCount) {
          result.put(metaData.getColumnName(i), rs.getObject(i))
        }
      }
      rs.close()
      ps.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result.toMap
  }
}
