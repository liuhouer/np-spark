package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object HotWordStats {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 设置检查点，保存状态。在生产中目录应该设置到HDFS
    ssc.checkpoint("data/checkpoint")

    // 创建DStream
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

    // DStream转换&输出
    // 每隔 10 秒，统计最近20秒的词出现的次数
    // window1 = t1 + t2 + t3
    // window2 = t3 + t4 + t5
    val wordCounts1: DStream[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKeyAndWindow((x: Int, y: Int) => x+y, Seconds(20), Seconds(10))
    wordCounts1.print()

    // window2 = w1 - t1 - t2 + t4 + t5
    // 需要checkpoint支持
    val wordCounts2: DStream[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKeyAndWindow(_+_, _-_, Seconds(20), Seconds(10))
    wordCounts2.print()

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
