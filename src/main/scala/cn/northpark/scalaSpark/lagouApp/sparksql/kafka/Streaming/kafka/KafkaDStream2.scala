package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDStream2 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 定义kafka相关参数
    val groupId: String = "mygroup01"
    val topics: Array[String] = Array("topicB")
    val kafkaParams: Map[String, Object] = getKafkaConsumerParameters(groupId)
    val offsets: Map[TopicPartition, Long] = Map(
      new TopicPartition("topicB", 0) -> 1000,
      new TopicPartition("topicB", 1) -> 1000,
      new TopicPartition("topicB", 2) -> 1000
    )

    // 创建DStream
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    // DStream转换&输出
    dstream.foreachRDD{ (rdd, time) =>
      // 处理消息
      println(s"*********** rdd.count = ${rdd.count()}; time = $time *************")

      // 显示offsets的信息
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{ iter =>
        val range: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${range.topic} ${range.partition}  ${range.fromOffset}  ${range.untilOffset} ")
      }
    }

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaConsumerParameters(groupid: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux121:9092,linux122:9092,linux123:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean)
//      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
  }
}