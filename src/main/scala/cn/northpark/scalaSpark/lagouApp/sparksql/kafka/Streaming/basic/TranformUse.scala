package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object TranformUse {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("data/checkpoint/")
    var i = 0
    println(s"i1 = $i")

    // 创建DStream
    val lines: DStream[String] = ssc.socketTextStream("localhost", 9999)

    // DStream转换
    val pairsDStream: DStream[(String, Int)] = lines.flatMap(_.split("\\s+"))
      .map((_, 1))

    // DStream输出
    lines.foreachRDD{rdd =>
      i += 1
    }
    println(s"i2 = $i")

    pairsDStream.foreachRDD{rdd =>
      println(s"i3 = $i")
    }
    println(s"i4 = $i")

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
