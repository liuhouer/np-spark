package cn.northpark.scalaSpark.lagouApp.student1.work02

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 首先编写日期准换函数将 15/Feb/2017:05:23:39 转换位 2017-2-15 05 即：年-月-日 时
 *
 * 1. 计算独立IP数:
 * 释义：每天的ip数量（去重）
 *
 * 2. 统计每个视频独立IP数（视频的标志：在日志文件的某些可以找到 *.mp4，代表一个视频文件）
 * 思路：过滤不含'.mp4'的数据 -> 根据日期,url分组 -> 统计去重后的ip
 *
 * 3.统计一天中每个小时的流量
 * 思路：根据日期分组 ->统计ip
 */
object LogAnalyse {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.init)
            .getOrCreate()

        spark.conf.set("spark.debug.maxToStringFields",100)

        spark.read
            .option("header", "false")
            .option("inferSchema", "true")
            .option("delimiter", " ")
            .csv("src/data/cdn.txt")
            .select("_c0", "_c3", "_c5")
            .toDF("ip", "reqTime", "url")
            .coalesce(1)
            .createOrReplaceTempView("logs")

        spark.udf.register("dateParse", dateParse _)

        // 1.独立IP数
        spark.sql(
            """
              |select to_date(dateParse(reqTime)) as time, count(distinct(ip)) as ct
              |from logs
              |where dateParse(reqTime) != 'error'
              |group by time
              |""".stripMargin)
            .write
            .mode("overwrite")
            .format("csv")
            .save("src/data/csv/02/01")

        // 2.统计每个视频独立IP数
        spark.sql(
            """
              |select to_date(dateParse(reqTime)) as time, count(distinct(ip)) as ct, url
              |from logs
              |where dateParse(reqTime) != 'error' and url like '%.mp4%'
              |group by time, url
              |""".stripMargin)
            .write
            .mode("overwrite")
            .format("csv")
            .save("src/data/csv/02/02")

        // 3.统计一天中每个小时的流量
        spark.sql(
            """
              |select dateParse(reqTime) as time, count(ip)
              |from logs
              |where dateParse(reqTime) != 'error'
              |group by time
              |""".stripMargin)
            .write
            .mode("overwrite")
            .format("csv")
            .save("src/data/csv/02/03")

        spark.stop()
    }

    /**
     * 自定义UDF，日期准换函数，例： 15/Feb/2017:05:23:39 转换位 2017-2-15 05 即：年-月-日 时
     * @param str 日期字符串
     * @return yyyy-MM-dd hh 格式的日期字符串 或 error
     */
    def dateParse(str: String): String = {
        val sdf1 = new SimpleDateFormat("[dd/MMM/yyyy:hh:mm:ss", Locale.ENGLISH)
        val sdf2 = new SimpleDateFormat("yyyy-MM-dd hh")
        val date = sdf2.format(sdf1.parse(str))
        val currentYear: Int = LocalDate.now.getYear
        val year: Int = date.split("-")(0).toInt
        if (year < currentYear && year > 1970) date
        else "error"
    }
}
