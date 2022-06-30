package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 直接过滤数据，效率最高，没有shuffle
object BlackListFilter3 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    // 自定义黑名单数据
    val blackList = Array(("spark", true), ("scala", false), ("hello", true), ("world", true))
      .filter(_._2)
      .map(_._1.toLowerCase)


    // 创建DStream。使用 ConstantInputDStream 用于测试
    val strArray: Array[String] = "Hello World Hello Hadoop Hello spark kafka hive zookeeper hbase flume sqoop scala"
      .split("\\s+")
      .zipWithIndex
      .map { case (word, timestamp) => s"$timestamp $word" }
    val rdd: RDD[String] = ssc.sparkContext.makeRDD(strArray)
    val wordDStream: ConstantInputDStream[String] = new ConstantInputDStream(ssc, rdd)

    // 流式数据的处理和输出
    wordDStream.map(line => (line.split("\\s+")(1).toLowerCase, line))
        .filter{case (word, _) => ! blackList.contains(word)}
        .map(_._2)
        .print(20)

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
