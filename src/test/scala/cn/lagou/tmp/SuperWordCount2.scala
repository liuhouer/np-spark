package cn.lagou.tmp

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SuperWordCount2 {
  val stopword = List("the", "a", "in", "and", "to", "of", "for", "is", "are", "on", "you", "can", "your", "as")
  val punctuation = """[,\\.?;:!"'()]"""

  def main(args: Array[String]): Unit = {
    // 1、创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 2、生成RDD
    val lines: RDD[String] = sc.textFile("file:///C:\\Project\\LagouBigData\\data\\swc.dat")

    // 3、RDD转换
    // 单词切分、转为小写
    val words: RDD[String] = lines.flatMap(_.split("\\s+"))
      .map(_.trim.toLowerCase())

    // 去停用词、标点
    val cleanWords: RDD[String] = words.filter(!stopword.contains(_))
      .map(_.replaceAll(punctuation, ""))

    // 计算词频，并按降序排序
    val result: RDD[(String, Int)] = cleanWords.map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    // 4、结果输出
    result.foreach(println)
//    result.saveAsTextFile("file:///C:\\Project\\LagouBigData\\data\\wcoutput")

    // 数据输出到 MySQL
    val username = "hive"
    val password = "12345678"
    val url = "jdbc:mysql://linux123:3306/ebiz?useUnicode=true&characterEncoding=utf-8&useSSL=false"

    var conn: Connection = null
    var stmt: PreparedStatement = null
    val sql = "insert into wordcount values (?, ?)"

    result.foreach{case (word, count) =>
      try{
        conn = DriverManager.getConnection(url, username, password)
        stmt = conn.prepareStatement(sql)
        stmt.setString(1, word)
        stmt.setInt(2, count)
        stmt.executeUpdate()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }
    }

    // 5、关闭SparkContext
    sc.stop()
  }
}
