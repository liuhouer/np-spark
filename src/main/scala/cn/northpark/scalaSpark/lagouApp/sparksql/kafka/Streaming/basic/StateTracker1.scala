package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object StateTracker1 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("data/checkpoint/")

    // 创建DStream
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

    // DStream转换
    val pairsDStream: DStream[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map((_, 1))

    // updateFunc: (Seq[V], Option[S]) => Option[S]
    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (currValues: Seq[Int], prevValues: Option[Int]) => {
      val currentSum = currValues.sum
      val prevSum: Int = prevValues.getOrElse(0)
      Some(currentSum + prevSum)
    }

    val resultDStream: DStream[(String, Int)] = pairsDStream.updateStateByKey[Int](updateFunc)
    resultDStream.cache()

    // DStream输出
    resultDStream.print()
    resultDStream
      .saveAsTextFiles("data/output1/")

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
