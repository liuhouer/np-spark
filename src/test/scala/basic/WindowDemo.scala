package basic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)

    // 每 5s 生成一个RDD（mini-batch）
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("error")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 1521)
    lines.foreachRDD{ (rdd, time) =>
      println(s"rdd = ${rdd.id}; time = $time")
      rdd.foreach(value => println(value))
    }

    // 20s 窗口长度(ds包含窗口长度范围内的数据)；10s 滑动间隔(多次时间处理一次数据)
    val res1: DStream[String] = lines.reduceByWindow(_ + " " + _, Seconds(20), Seconds(10))
    res1.print()

    val res2: DStream[String] = lines.window(Seconds(20), Seconds(10))
    res2.print()

    // 求窗口元素的和
    val res3: DStream[Int] = lines.map(_.toInt).reduceByWindow(_+_, Seconds(20), Seconds(10))
    res3.print()

    // 求窗口元素的和
    val res4 = res2.map(_.toInt).reduce(_+_)
    res4.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
