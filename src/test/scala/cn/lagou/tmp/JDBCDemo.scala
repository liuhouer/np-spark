package cn.lagou.tmp

import java.sql.{Connection, DriverManager, PreparedStatement}

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    // 定义结果集
    val str = "hadoop spark java scala hbase hive sqoop hue tez atlas datax grinffin zk kafka"
    val result: Array[(String, Int)] = str.split("\\s+").zipWithIndex

    // 定义连接信息
    val username = "hive"
    val password = "12345678"
    val url = "jdbc:mysql://linux123:3306/ebiz?useUnicode=true&characterEncoding=utf-8&useSSL=false"

    var conn: Connection = null
    var stmt: PreparedStatement = null
    val sql = "insert into wordcount values (?, ?)"
    try{
      conn = DriverManager.getConnection(url, username, password)
      stmt = conn.prepareStatement(sql)
      result.foreach{case (word, count) =>
        stmt.setString(1, word)
        stmt.setInt(2, count)
        stmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (stmt != null) stmt.close()
      if (conn != null) conn.close()
    }
  }
}
