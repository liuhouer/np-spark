package cn.northpark.scalaSpark.lagouApp.spark

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *日志数据分析
 * */
object LogAnalysis {
  val ipVedioPattern =  Pattern.compile("""(\S*) .*/(\S*\.mp4) .*""")
  val timeFlowPattern =  Pattern.compile(""".* \[(.*)\] .* (200|206|304) (\d*) .*""")

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //读取日志
    val lines:RDD[String] = sc.textFile("src\\data\\cdn.txt")
    val IPrdd:RDD[(String,Int)] = lines.map({
        line=>
          val fields = line.split("\\s+")
          (fields(0),1)
    })
    // 计算独立IP
    IPrdd.reduceByKey(_ + _).sortBy(_._2,false).take(10).foreach(println)
    println("***************************")

    //计算每个视频独立IP数
    val videoRDD:RDD[((String,String),Int)] = lines.map(line=>{
      val matcher: Matcher = ipVedioPattern.matcher(line)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val video =  matcher.group(2)
        ((video, ip), 1)
      } else {
        (("", ""), 1)
      }
    })
    videoRDD.reduceByKey(_ + _).map{ case ((video,ip),count)=>(video,1)}
        .reduceByKey(_+_)
        .take(10)
        .foreach(println)

    println("***************************")
    //计算一天中每个小时流量
    val timeFlowRDD: RDD[(String, Long)] = lines.map(line => {
      val matcher: Matcher = timeFlowPattern.matcher(line)
      if (matcher.matches()) {
        val time = matcher.group(1)
        val hour: String = time.split(":")(1)
        val flow: Long = matcher.group(3).toLong
        (hour, flow)
      } else ("", 0L)
    })
    timeFlowRDD.reduceByKey(_+_).sortBy(_._1,false).take(10).foreach(println)
    // 关闭SparkContext
    sc.stop()
  }
}
