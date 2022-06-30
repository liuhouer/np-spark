package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileDStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    // 创建StreamingContext
    // StreamingContext是所有流功能函数的主要访问点，这里使用多个执行线程和2秒的批次间隔来创建本地的StreamingContext
    // 时间间隔为2秒,即2秒一个批次
    val ssc = new StreamingContext(conf, Seconds(5))

    // 这里采用本地文件，也可以采用HDFS文件
    val lines = ssc.textFileStream("data/log/")
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    // 打印单位时间所获得的计数值
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}