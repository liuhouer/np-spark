package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StateTracker2 {
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

    // (KeyType, Option[ValueType], State[StateType]) => MappedType
    def mappingFunction(key: String, one: Option[Int], state: State[Int]): (String, Int) = {
      // 计算value
      val sum = one.getOrElse(0) + state.getOption().getOrElse(0)
      // 保存状态
      state.update(sum)
      // 输出值
      (key, sum)
    }
    val spec = StateSpec.function(mappingFunction _)
    val resultDStream: DStream[(String, Int)] = pairsDStream.mapWithState[Int, (String, Int)](spec)
      // 显示快照（显示所有相关的key-value）
      .stateSnapshots()
    resultDStream.cache()

    // DStream输出
    resultDStream.print()
    resultDStream.saveAsTextFiles("data/output2/")

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }
}
