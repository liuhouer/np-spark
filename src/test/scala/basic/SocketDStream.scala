package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketDStream {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SocketStream").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split("\\s+"))
    val wordCounts = words.map(x => (x.trim, 1)).reduceByKey(_ + _)

    // 打印单位时间所获得的计数值
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
