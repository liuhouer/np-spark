package cn.northpark.scalaSpark.lagouApp.student2

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object work2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName.init)
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("warn")


    val cdnRDD: RDD[String] = sc.textFile("data/cdn.txt")

    //缓存，避免重复读取
    cdnRDD.cache()

    singleIp(cdnRDD)
    vedioIp(cdnRDD)
    requestFlow(cdnRDD)


    spark.close()
  }



  def singleIp(cdnRDD:RDD[String])={
    val ips = cdnRDD.map(line => {
      val lines = line.split("\\s+")
      (lines(0), 1)
    }).reduceByKey(_ + _)
    println(s"独立IP个数：${ips.count()}")

  }

  def vedioIp(cdnRDD:RDD[String]): Unit ={
    val ipPattern = Pattern.compile("""(\S+) .+/(\S+\.mp4) .*""")
    val vedioIPRDD: RDD[(String, Int)] = cdnRDD.map { line =>
      val matcher: Matcher = ipPattern.matcher(line)
      if (matcher.matches()) {
        ((matcher.group(2), matcher.group(1)), 1)
      } else {
        (("", ""), 0)
      }
    }.filter {
      case ((vedio, ip), count) => vedio != "" && ip != "" && count > 0
    }.reduceByKey(_ + _)
      .map {
        case ((vedio, ip), count) => (vedio, 1)
      }.reduceByKey(_ + _)
    println(s"每个视频独立IP数：${vedioIPRDD.count()}")
  }

  def requestFlow(cdnRDD:RDD[String]): Unit ={
    val flowPattern = Pattern.compile(""".+ \[(.+?) .+ (200|206|304) (\d+) .+""")
    val flowRDD: RDD[(String, Long)] = cdnRDD.map { line => {
      val matcher: Matcher = flowPattern.matcher(line)
      if (matcher.matches()) {
        (matcher.group(1).split(":")(1), matcher.group(3).toLong)
      } else {
        ("", 0L)
      }
    }
    }.filter {
      case (hourse, flow) => hourse != "" && flow > 0
    }
      .reduceByKey(_ + _)
      .sortBy(_._1)
    println(s"一天中每个小时的流量：")
    flowRDD.foreach(println)
  }

}