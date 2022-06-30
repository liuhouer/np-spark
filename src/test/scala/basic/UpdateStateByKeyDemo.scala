package basic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyDemo {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.checkpoint("data/checkpoint/")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split("\\s+"))
    val wordDstream: DStream[(String, Int)] = words.map(x => (x, 1))

    // 定义状态更新函数
    // 函数常量定义，返回类型是Some(Int)，表示的含义是最新状态
    // 函数的功能是将当前时间间隔内产生的Key的value集合，加到上一个状态中，得到最新状态
    val updateFunc = (currValues: Seq[Int], prevValueState: Option[Int]) => {
      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq，再计算当前批次的总和
      val currentCount = currValues.sum
      // 已累加的值
      val previousCount = prevValueState.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val stateDstream: DStream[(String, Int)] = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()

    // 把DStream保存到文本文件中，会生成很多的小文件。一个批次生成一个目录
    val outputDir = "data/output1"
    stateDstream.repartition(1)
      .saveAsTextFiles(outputDir)

    ssc.start()
    ssc.awaitTermination()
  }
}
