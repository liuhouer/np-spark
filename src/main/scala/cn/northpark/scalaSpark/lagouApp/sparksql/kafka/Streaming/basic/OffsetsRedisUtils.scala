package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable

object OffsetsRedisUtils {
  private val config = new JedisPoolConfig
  private val redisHost = "192.168.80.123"
  private val redisPort = 6379
  // 最大连接
  config.setMaxTotal(30)
  // 最大空闲
  config.setMaxIdle(10)

  private val pool = new JedisPool(config, redisHost, redisPort, 10000)

  private val topicPrefix = "kafka:topic"

  // key的格式为 => prefix : topic : groupId
  private def getKey(topic: String, groupId: String, prefix: String = topicPrefix): String = s"$prefix:$topic:$groupId"

  private def getRedisConnection: Jedis = pool.getResource

  // 从 redis 中获取offsets
  def getOffsetFromRedis(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = getRedisConnection

    val offsets: Array[mutable.Map[TopicPartition, Long]] = topics.map { topic =>
      import scala.collection.JavaConverters._
      jedis.hgetAll(getKey(topic, groupId))
        .asScala
        .map { case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong }
    }
    // println(s"offsets = ${offsets.toBuffer}")

    // 归还资源
    jedis.close()

    offsets.flatten.toMap
  }

  // 将 offsets 保存到 redis
  def saveOffsetsToRedis(ranges: Array[OffsetRange], groupId: String): Unit = {
    val jedis: Jedis = getRedisConnection

    ranges.map(range => (range.topic, range.partition -> range.untilOffset))
      .groupBy(_._1)
      .map { case (topic, buffer) => (topic, buffer.map(_._2)) }
      .foreach { case (topic, partitionAndOffset) =>
        val offsets: Array[(String, String)] = partitionAndOffset.map(elem => (elem._1.toString, elem._2.toString))

        import scala.collection.JavaConverters._

        jedis.hmset(getKey(topic, groupId), offsets.toMap.asJava)
      }

    // 归还资源
    jedis.close()
  }

  def main(args: Array[String]): Unit = {
    val topics = Array("topicB")
    val groupId = "group01"
    val offsets: Map[TopicPartition, Long] = getOffsetFromRedis(topics, groupId)
    println(offsets)
    offsets.foreach(println)

    val jedis: Jedis = getRedisConnection
    import scala.collection.JavaConverters._
    jedis.hgetAll(getKey("topicB", groupId)).asScala.foreach(println)
  }
}
