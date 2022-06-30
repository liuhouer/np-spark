package cn.northpark.scalaSpark.lagouApp.sparksql.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

// 使用自定义的offsets，从kafka读数据；处理完数据后打印offsets
object KafkaDStream3 {
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

    // 从 Redis 获取offsets
    val offsets: collection.Map[TopicPartition, Long] = OffsetsRedisUtils.getOffsetFromRedis(topics, "")

    // 从 kafka 中获取数据
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    // DStream输出
    dstream.foreachRDD { rdd =>
      // 用数组存放所有分区的offset信息，数组的下标为RDD partitionID
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition{ iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }

      // 显示数据
      println(s"rdd.count = ${rdd.count}")

      // 处理完数据之后，将 offsets 保存到redis中
      OffsetsRedisUtils.saveOffsetsToRedis(offsetRanges)
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