package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

object WindowDemo {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 创建DStream
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

    // DStream转换和输出
    // foreachRDD输出
//    lines.foreachRDD{(rdd, time) =>
//      println(s"rdd = ${rdd.id}; time = $time")
//      rdd.foreach(println)
//    }

    // 窗口操作
    val res1: DStream[String] = lines.reduceByWindow(_ + " " + _, Seconds(20), Seconds(10))
    res1.print()

    val res2: DStream[String] = lines.window(Seconds(20), Seconds(10))
    res2.print()

    val res3: DStream[Int] = res2.map(_.toInt).reduce(_ + _)
    res3.print()

    val res4: DStream[Int] = lines.map(_.toInt).reduceByWindow(_+_, Seconds(20), Seconds(10))
    res4.print()

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
