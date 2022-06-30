package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


// transform
// ConstantInputDStream 主要用于测试
object BlackListFilter1 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 自定义黑名单数据
    val blackList = Array(("spark", true), ("scala", false), ("hello", true), ("world", true))
      .map(elem => (elem._1.toLowerCase, elem._2))
    val blackListRDD: RDD[(String, Boolean)] = ssc.sparkContext.makeRDD(blackList)

    // 创建DStream。使用 ConstantInputDStream 用于测试
    val strArray: Array[String] = "Hello World Hello Hadoop Hello spark kafka hive zookeeper hbase flume sqoop scala"
      .split("\\s+")
      .zipWithIndex
      .map { case (word, timestamp) => s"$timestamp $word" }
    val rdd: RDD[String] = ssc.sparkContext.makeRDD(strArray)
    val wordDStream: ConstantInputDStream[String] = new ConstantInputDStream(ssc, rdd)

    // 流式数据的处理
    wordDStream.transform{rdd =>
      rdd.map{line => (line.split("\\s+")(1).toLowerCase, line)}
        .leftOuterJoin(blackListRDD)
          .filter{case (_, (_, rightValue)) => ! rightValue.getOrElse(false)}
          .map{case (_, (leftValue, _)) => leftValue}
    }.print(20)

    // 流式数据的输出

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
