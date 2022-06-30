package cn.northpark.scalaSpark.lagouApp.student

import org.apache.spark.rdd.RDD

import java.util.regex.{Matcher, Pattern}
import org.apache.spark.{SparkConf, SparkContext}

object HomeWork2 {
  // 属于视频的网址
  val ipPattern = Pattern.compile("""(\S+) .+/(\S+\.mp4) .*""")
  // 正常返回的请求
  val flowPattern = Pattern.compile(""".+ \[(.+?) .+ (200|206|304) (\d+) .+""")

  def main(args: Array[String]): Unit = {
    // 初始化,框架代码
    val conf = new SparkConf().setAppName("HomeWork2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 设置日志级别
    sc.setLogLevel("WARN")

    // 读取日志文件，获取每行数据格式的rdd
    val logRDD: RDD[String] = sc.textFile("data/cdn.txt")

    // 获取第一列中的ip，并统计该ip出现的总数，降序排列
    val ipRDD: RDD[(String, Int)] = logRDD.map(line => (line.split("\\s+")(0), 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    // 打印相关信息
    println("ip出现次数，降序排列前十:")
    ipRDD.take(10).foreach(println)
    println(s"独立IP总数：${ipRDD.count()}")

    //////////////////////////////////

    // 查找请求属于视频请求的行
    val videoIpRDD: RDD[((String, String), Int)] = logRDD.map(line => {
      val matchFlag: Matcher = ipPattern.matcher(line)
      if (matchFlag.matches()) {
        // 正则匹配，获取视频名称，获取地址
        ((matchFlag.group(2), matchFlag.group(1)), 1)
      } else
        (("", ""), 0)
    })

    println("视频独立IP数，降序排列前十：")
    // 过滤无效数据
    videoIpRDD.filter { case ((video, ip), count) => video != "" && ip != "" && count != 0 }
      // 对视频数据进行统计数量
      .reduceByKey(_ + _)
      // 因为已经过滤掉无效数据，这里key更换为视频名称
      .map { case ((video, _), _) => (video, 1) }
      // 相同的视频不同的ip这里再次做统计，因为查询的是视频的访问量
      .reduceByKey(_ + _)
      // 根据访问数进行统计
      .sortBy(_._2, false)
      // 取前十
      .take(10)
      .foreach(println)
    println(s"视频独立IP总数：${videoIpRDD.count()}")

    //////////////////////////////////

    // 正则获取每行数据的当前小时和响应大小
    val flowRDD: RDD[(String, Long)] = logRDD.map(line => {
      val matchFlag = flowPattern.matcher(line)
      if (matchFlag.matches())
        (matchFlag.group(1).split(":")(1), matchFlag.group(3).toLong)
      else
        ("", 0L)
    })

    println("每小时流量:")
    flowRDD.filter { case (hour, flow) => flow != 0 }
      // 数据量很小，可以收到一个分区中做reduce，然后转为集合操作效率高
      .reduceByKey(_ + _, 1)
      .collectAsMap()
      // 响应大小更换单位为 g
      .mapValues(_ / 1024 / 1024 / 1024)
      .toList
      // 根据小时排序
      .sortBy(_._1)
      .foreach { case (k, v) => println(s"${k}时 CDN流量${v}G") }

    sc.stop()
  }
}
