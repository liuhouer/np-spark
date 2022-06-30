package kafka010

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDemo01 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getCanonicalName)
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams: Map[String, Object] = getKafkaConsumerParams()
    val topics: Array[String] = Array("topicB")

    // PreferBrokers: Use this only if your executors are on the same nodes as your Kafka brokers.
    // PreferConsistent: Use this in most cases, it will consistently distribute partitions across all executors.
    // PreferFixed: Use this to place particular TopicPartitions on particular hosts if your load is uneven.
    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    // Subscribe 或 SubscribePattern，订阅topic；有自动分区发现功能(运行流期间添加分区)
    // Assign 指定一个固定的分区集合。无自动分区发现功能【没有特殊需求少用】

    dstream.foreachRDD{(rdd, time) =>
      if (!rdd.isEmpty()) {
        println(s"*********** rdd.count = ${rdd.count()}; time = $time ***********")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaConsumerParams(groupId: String = "mygroup1"): Map[String, Object] = {
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "linux121:9092,linux122:9092,linux123:9092",
      // org.apache.kafka.common.serialization.StringDeserializer
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (false: java.lang.Boolean))
  }
}


//    stream.foreachRDD { rdd =>
//      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      offsetRanges.foreach(offset => {
//        println(s"topic = ${offset.topic}, partition = ${offset.partition}, fromOffset = ${offset.fromOffset}, untilOffset = ${offset.untilOffset}")
//      })
//    }