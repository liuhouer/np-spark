package cn.northpark.scalaSpark.lagouApp.sparksql.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object OffsetsRedisUtils {
  private val config = new JedisPoolConfig
  private val redisHost = "linux123"
  private val redisPort = 6379
  // 最大连接
  config.setMaxTotal(30)
  // 最大空闲
  config.setMaxIdle(10)
  private val pool = new JedisPool(config, redisHost, redisPort, 10000)

  private val topicPrefix = "kafka:topic"

  // key的格式为 => prefix : topic : groupId
  private def getKey(topic: String, groupId: String = "", prefix: String = topicPrefix): String = s"$prefix:$topic:$groupId"

  private def getRedisConnection: Jedis = pool.getResource

  // 从 redis 中获取offsets
  def getOffsetFromRedis(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    val jedis = getRedisConnection
    val offsets = for (topic <- topics) yield {
      import scala.collection.JavaConversions._

      jedis.hgetAll(getKey(topic, groupId)).toMap
        .map { case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong }
    }
    // 归还资源
    jedis.close()
    offsets.flatten.toMap
  }

  // 将 offsets 保存到 redis
  def saveOffsetsToRedis(range: Array[OffsetRange], groupId: String = ""): Unit = {
    val jedis = getRedisConnection
    val offsets = for (range <- range) yield {
      (range.topic, range.partition -> range.untilOffset)
    }
    val offsetsMap: Map[String, Map[Int, Long]] = offsets.groupBy(_._1).map { case (topic, buffer) => (topic, buffer.map(_._2).toMap) }

    for ((topic, partitionAndOffset) <- offsetsMap) {
      val offsets = partitionAndOffset.map(elem => (elem._1.toString, elem._2.toString))
      import scala.collection.JavaConversions._
      jedis.hmset(getKey(topic, groupId), offsets)
    }

    // 归还资源
    jedis.close()
  }
}