package basic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HotWordStats {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    //设置检查点，检查点具有容错机制。生产环境中应设置到HDFS
    ssc.checkpoint("data/checkpoint/")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split("\\s+"))
    val pairs: DStream[(String, Int)] = words.map(x => (x, 1))

    // 通过reduceByKeyAndWindow算子, 每隔10秒统计最近20秒的词出现的次数
    // 后 3个参数：窗口时间长度、滑动窗口时间、分区
    val wordCounts1: DStream[(String, Int)] = pairs.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,
      Seconds(20),
      Seconds(10), 2)
    wordCounts1.print

    // 这里需要checkpoint的支持
    val wordCounts2: DStream[(String, Int)] = pairs.reduceByKeyAndWindow(
      _ + _,
      _ - _,
      Seconds(20),
      Seconds(10), 2)
    wordCounts2.print

    ssc.start()
    ssc.awaitTermination()
  }
}
