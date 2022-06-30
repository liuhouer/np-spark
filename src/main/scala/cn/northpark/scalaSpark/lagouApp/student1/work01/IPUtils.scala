package cn.northpark.scalaSpark.lagouApp.student1.work01

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/*
* 要求：将 http.log 文件中的 ip 转换为地址。如将 122.228.96.111 转为温州，并统计各城市的总访问量*/

object IPUtils {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    //创建SparkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName.init)
      .getOrCreate()
    import spark.implicits._
    //读取ip.dat文件
    val df1: Array[(Long, Long, String)] = spark.read
      .option("delimiter", "|")
      .csv("src/data/ip.dat")
      .map(row => (row.getString(2).toLong, row.getString(3).toLong, row.getString(7)))
      .collect()
    val ipBC: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(df1.sortBy(_._1))

    // 读取http.log文件
    spark.read
      .option("delimiter", "|")
      .csv("src/data/http.log")
      .coalesce(1)
      .map(row => row.getString(1))
      .createOrReplaceTempView("tb_ip")

    /**
     * 自定义UDF函数将IP地址转数字地址
     *
     * @param ip IP地址
     * @return 数字地址
     */
    def ip2Long(ip: String): Long = {
      ip.split("\\.").map(_.toLong).fold(0L) {
        (buffer, element) => buffer << 8 | element
      }
    }

    /**
     *自定义UDF函数获取城市
     * @param ip ip数字地址
     * @return ip所在城市
     */
    def getCity(ip: Long): String = {
      val ipInfo = ipBC.value
      var start = 0
      var end = ipInfo.length - 1
      var flag = 0

      while (start <= end) {
        flag = (start + end) / 2
        if ((ip >= ipInfo(flag)._1) && (ip <= ipInfo(flag)._2)) return ipInfo(flag)._3
        else if (ip < ipInfo(flag)._1) end = flag - 1
        else start = flag + 1
      }
      "unknown"
    }

    spark.udf.register("ip2Long", ip2Long _)
    spark.udf.register("getCity", getCity _)

    spark.sql(
      """
        |select getCity(ip2Long(value)) as city, count(1)
        |from tb_ip
        |group by city
        |""".stripMargin)
      .write
      .mode("overwrite")
      .format("csv")
      .save("src/data/csv/01")

    spark.stop()
  }
}
