package basic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object MapWithStateDemo {
  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    ssc.checkpoint("data/checkpoint/")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val words: DStream[String] = lines.flatMap(_.split("\\s+"))
    val wordDstream: DStream[(String, Int)] = words.map(x => (x, 1))

    // 函数返回的类型即为 mapWithState 的返回类型
    val mappingFunc: (String, Option[Int], State[Int]) => (String, Int) = (word: String, one: Option[Int], state: State[Int]) => {
      val sum: Int = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output: (String, Int) = (word, sum)
      state.update(sum)
      output
    }
    val stateDStream: DStream[(String, Int)] = wordDstream.mapWithState[Int, (String, Int)](StateSpec.function(mappingFunc))
    stateDStream.cache()
    stateDStream.print()

    // 把DStream保存到文本文件中，会生成很多的小文件。一个批次生成一个目录
    val outputDir = "data/output1"
    stateDStream.repartition(1)
      .saveAsTextFiles(outputDir)

    ssc.start()
    ssc.awaitTermination()
  }
}
