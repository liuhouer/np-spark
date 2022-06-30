package cn.northpark.scalaSpark.lagouApp.student3

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FindIparea {



  /**
   * 题目：
   * http.log：用户访问网站所产生的日志。日志格式为：时间戳、IP地址、访问网址、访问数据、浏览器信息等
   * ip.dat：ip段数据，记录着一些ip段范围对应的位置
   * 文件位置：data/http.log、data/ip.dat
   * 要求：将 http.log 文件中的 ip 转换为地址。如将 122.228.96.111 转为 温州，并统计各城市的总访问量
   *
   *  1. 提取文件的有用信息字段
   *      http.log 提取访问的IP
   *      ip.dat 提取IP段和对应的城市 （可优化，广播变量）
   *  2. 使用IP 与 IP段匹配，得到城市
   *      查找的过程可优化(使用二分查找)
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("FindIparea").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ipRdd: RDD[String] = parseHttpLog(sc)
    val citiesRdd: RDD[(Long, Long, String)] = parseIpDat(sc)
    // 将该表广播, 排序
    val critiesBC: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(citiesRdd.collect().sortBy(_._1))
    val resRdd: RDD[(String, Int)] = matchCities(ipRdd, critiesBC)
    resRdd.reduceByKey(_ + _).collect().foreach(println)
    // Thread.sleep(10000000)
    sc.stop()
  }

  /** 解析http.log 得到 ip */
  def parseHttpLog(sc: SparkContext): RDD[String] = {
    val ipRdd: RDD[String] = sc.textFile("C:\\Users\\86178\\Desktop\\拉勾正式班\\大数据正式班第四阶段模块三\\LagouBigData\\src\\data\\http.log").map(_.split("\\|")(1))
    ipRdd
  }

  /** 解析IP.dat 得到 IP段以及城市名 */
  def parseIpDat(sc: SparkContext): RDD[(Long, Long, String)] = {
    sc.textFile("C:\\Users\\86178\\Desktop\\拉勾正式班\\大数据正式班第四阶段模块三\\LagouBigData\\src\\data\\ip.dat").map{line =>
      val array: Array[String] = line.split("\\|")
      if (StringUtils.isNotEmpty(array(7))){
        // 存在部分数据位置不同，如香港
        (array(2).toLong, array(3).toLong, array(7))
      } else {
        (array(2).toLong, array(3).toLong, array(6))
      }
    }
  }

  /** 将ip地址转换为long数字 */
  def changeToNum(ip: String): Long = {
    ip.split("\\.")
      .map(_.toLong)
      .fold(0L){(buffer, elem) =>
        buffer << 8 | elem
      }
  }

  /** 二分查找匹配的城市 */
  def selectCite(num: Long, crities: Array[(Long, Long, String)]): String = {
    var start = 0
    var end = crities.length - 1
    var middle = 0
    while (start <= end) {
      middle = (start + end) / 2
      if ((num >= crities(middle)._1) && (num <= crities(middle)._2)) {
        return crities(middle)._3
      } else if (num > crities(middle)._2) {
        start = middle + 1
      } else {
        end = middle - 1
      }
    }
    "Unkown"
  }

  /** 匹配城市主体方法 */
  def matchCities(ipRdd: RDD[String], critiesBC: Broadcast[Array[(Long, Long, String)]]) = {
    val crities: Array[(Long, Long, String)] = critiesBC.value
    ipRdd.mapPartitions(iter => {
      iter.map(x => {
        val num: Long = changeToNum(x)
        (selectCite(num, crities), 1)
      })
    })
  }
}
