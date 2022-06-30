package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketDStream {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    // 创建DStream
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

    // DStream转换
    val words: DStream[String] = lines.flatMap(_.split("\\s+"))
    val result: DStream[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _)

    // DStream输出
    result.print(20)

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
