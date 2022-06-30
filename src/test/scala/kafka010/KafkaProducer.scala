package kafka010

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    // 定义 kafka 参数
    val brokers = "linux121:9092,linux122:9092,linux123:9092"
    val topic = "topicB"
    val prop = new Properties()

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    // KafkaProducer
    val producer = new KafkaProducer[String, String](prop)

    for (i <- 1 to 1000000){
      val msg = new ProducerRecord[String, String](topic, i.toString, i.toString)
      // 发送消息
      producer.send(msg)
      println(s"i = $i")
      Thread.sleep(100)
    }

    producer.close()
  }
}