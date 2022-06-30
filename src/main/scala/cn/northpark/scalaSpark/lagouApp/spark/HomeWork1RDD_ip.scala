package cn.northpark.scalaSpark.lagouApp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HomeWork1RDD_ip {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 读数据，并解析。将ip地址转换为Long类型
    val httpData: RDD[Long] = sc.textFile("data/http.log")
      .map(x => ip2Long(x.split("\\|")(1)))

    // 读数据，解析，收回来。最后变为广播变量
    val ipData: Array[(Long, Long, String)] = sc.textFile("data/ip.dat")
      .map { line =>
        val field = line.split("\\|")
        (field(2).toLong, field(3).toLong, field(6))
      }.collect()
    val ipBC = sc.broadcast(ipData.sortBy(_._1))

    // 逐条数据比对，找到对应的城市。使用二分查找
    httpData.mapPartitions { iter =>
      val ipsInfo: Array[(Long, Long, String)] = ipBC.value
      iter.map { ip =>
        val city: String = getCityName(ip, ipsInfo)
        (city, 1)
      }
    }.reduceByKey(_ + _)
      .collect()
      .foreach(println)

    sc.stop()
  }

  // 将ip地址转换为Long。有多种实现方法，此方法使用了位运算，效率最高
  def ip2Long(ip: String): Long = {
    ip.split("\\.")
      .map(_.toLong)
      .fold(0L) { (buffer, elem) =>
        buffer << 8 | elem
      }
  }

  // 给定ip地址，在ips中查找对应的城市名。使用二分查找算法
  def getCityName(ip: Long, ips: Array[(Long, Long, String)]): String = {
    var start = 0
    var end = ips.length - 1
    var middle = 0

    while (start <= end) {
      middle = (start + end) / 2
      if ((ip >= ips(middle)._1) && (ip <= ips(middle)._2))
        return ips(middle)._3
      else if (ip < ips(middle)._1)
        end = middle - 1
      else
        start = middle + 1
    }
    "Unknown"
  }
}