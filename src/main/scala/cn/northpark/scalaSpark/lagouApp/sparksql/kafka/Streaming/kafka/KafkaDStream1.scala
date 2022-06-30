package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.kafka

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDStream1 {
  def main(args: Array[String]): Unit = {
    // 初始化
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("FileDStream").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 定义kafka相关参数
    val groupId: String = "mygroup01"
    val topics: Array[String] = Array("lg_bus_info2")
    val kafkaParams: Map[String, Object] = getKafkaConsumerParameters(groupId)

    // 创建DStream
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // DStream转换&输出
    dstream.foreachRDD{ (rdd, time) =>
      println(s"*********** rdd.count = ${rdd.count()}; time = $time *************")
    }

    // 启动作业
    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaConsumerParameters(groupid: String): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop3:9092,hadoop4:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
  }
}