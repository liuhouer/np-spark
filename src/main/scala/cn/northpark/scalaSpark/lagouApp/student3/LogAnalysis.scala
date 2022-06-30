package cn.northpark.scalaSpark.lagouApp.student3

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object LogAnalysis {
  /**
   * 题目：
   *      日志格式：IP 命中率(Hit/Miss) 响应时间 请求时间 请求方法 请求URL 请求协议 状态码 响应大小referer 用户代理
   *      日志文件位置：data/cdn.txt
   * 术语解释：
   *      PV(page view)，即页面浏览量；衡量网站或单一网页的指标
   *      uv(unique visitor)，指访问某个站点或点击某条新闻的不同IP地址的人数
   * 任务：
   *      2.1、计算独立IP数
   *      2.2、统计每个视频独立IP数（视频的标志：在日志文件的某些可以找到 *.mp4，代表一个视频文件）
   *      2.3、统计一天中每个小时的流量
   */
  val spark: SparkSession = SparkSession.builder().appName("LogAnalysis").master("local[*]").getOrCreate()
  Logger.getLogger("org").setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(LogAnalysis.getClass)
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  import spark.implicits._
  import org.apache.spark.sql.functions._
  def main(args: Array[String]): Unit = {

    val fileDF: DataFrame = spark.read.option("delimiter", " ").csv("src\\data\\cdn.txt")
    // fileDF.show(1)
    fileDF.cache()
    val ipNumberDF: Dataset[Row] = getIpNumber(fileDF)
    println(s"独立IP数： ${ipNumberDF.count()}")
    val parseUrlUdf: UserDefinedFunction = udf(parseUrl)
    println(s"mp4独立IP数： ${getVideoIpNumber(ipNumberDF, parseUrlUdf)}")
    println(getFlowEveryHours(fileDF).show())
  }

  /** 计算独立IP数 */
  def getIpNumber(fileDF: DataFrame) = {
    fileDF.dropDuplicates("_c0")
  }

  /** 统计每个视频独立IP数（视频的标志：在日志文件的某些可以找到 *.mp4，代表一个视频文件） */
  def getVideoIpNumber(ipNumberDF: DataFrame, parseUrlUdf: UserDefinedFunction): Long = {
    ipNumberDF.filter(parseUrlUdf($"_c5")).count()
  }

  /** 解析url的udf */
  val parseUrl = (columnVal: String) => {
    columnVal.contains(".mp4")
  }

  /** 统计一天中每个小时的流量 */
  def getFlowEveryHours(fileDF: DataFrame) = {
    spark.udf.register("parseTime", parseTime)
    val df: DataFrame = fileDF.selectExpr("parseTime(_c3, _c4) as hour", "_c7").filter("hour >= 0")
    df.groupBy("hour").agg(sum("_c7").as("sumFlow"))
  }

  /** 解析出小时 */
  val parseTime = (timeStr1: String, timeStr2: String) => {
    val timeString: String = timeStr1.concat(" ").concat(timeStr2)
    try {
      val time: LocalDateTime = LocalDateTime.parse(timeString.substring(1, timeString.length - 1), dateTimeFormatter)
      time.getHour
    } catch {
      case _: DateTimeParseException => {
        logger.error(s"日期解析错误 $timeString")
        -1
      }
    }
  }
}
