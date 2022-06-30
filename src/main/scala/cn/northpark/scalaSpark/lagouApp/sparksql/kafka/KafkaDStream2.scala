package cn.northpark.scalaSpark.lagouApp.sparksql.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 使用自定义的offsets，从kafka读数据；处理完数据后打印offsets
object KafkaDStream2 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(2))

    // 定义kafka相关参数
    val kafkaParams: Map[String, Object] = getKafkaConsumerParams()
    val topics: Array[String] = Array("topicB")

    // 从指定的位置获取kafka数据
    val offsets: collection.Map[TopicPartition, Long] = Map(
      new TopicPartition("topicB", 0) -> 100,
      new TopicPartition("topicB", 1) -> 200,
      new TopicPartition("topicB", 2) -> 300
    )

    // 从 kafka 中获取数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    // DStream输出
    dstream.foreachRDD{(rdd, time) =>
      // 输出结果
      println(s"*********** rdd.count = ${rdd.count()}; time = $time ***********")

      // 输出offset
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        // 输出kafka消费的offset
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaConsumerParams(groupId: String = "mygroup1"): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux121:9092,linux122:9092,linux123:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
  }
}

// kafka命令，检查 topic offset的值
// kafka-run-class.sh  kafka.tools.GetOffsetShell --broker-list linux121:9092,linux122:9092,linux123:9092 --topic topicB --time -1