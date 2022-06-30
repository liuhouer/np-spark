package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

object KafkaDStream3 {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 定义kafka相关参数
    val groupId: String = "lagou_group01"
    val topics: Array[String] = Array("lagou_topic01", "lagou_topic02")
    val kafkaParams: Map[String, Object] = getKafkaConsumerParameters(groupId)

    // 从 Redis中获取offsets
    val offsets: Map[TopicPartition, Long] = OffsetsWithRedisUtils.getOffsetsFromRedis(topics, groupId)
    offsets.foreach(println)

    // 创建DStream
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets)
    )

    // DStream转换&输出
    dstream.foreachRDD{ (rdd, time) =>
      if (! rdd.isEmpty()) {
        // 处理消息
        println(s"*********** rdd.count = ${rdd.count()}; time = $time *************")

        // 将offsets信息打印到控制台
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition{ iter =>
          val range: OffsetRange = offsetRanges(TaskContext.get.partitionId)
          println(s"${range.topic} ${range.partition}  ${range.fromOffset}  ${range.untilOffset} ")
        }

        // 将offsets保存到Redis
        OffsetsWithRedisUtils.saveOffsetsToRedis(offsetRanges, groupId)
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

// kafka-topics.sh --zookeeper linux121:2181,linux122:2181,linux123:2181 --create --topic lagou_topic01 --replication-factor 2 --partitions 3
// kafka-topics.sh --zookeeper linux121:2181,linux122:2181,linux123:2181 --create --topic lagou_topic02 --replication-factor 2 --partitions 2