package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.kafka

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.mutable

object OffsetsWithRedisUtils {
  // 定义Redis参数
  private val redisHost = "linux123"
  private val redisPort = 6379

  // 获取Redis的连接
  private val config = new JedisPoolConfig
  // 最大空闲数
  config.setMaxIdle(5)
  // 最大连接数
  config.setMaxTotal(10)

  private val pool = new JedisPool(config, redisHost, redisPort, 10000)
  private def getRedisConnection: Jedis = pool.getResource

  private val topicPrefix = "kafka:topic"

  // Key：kafka:topic:TopicName:groupid
  private def getKey(topic: String, groupid: String) = s"$topicPrefix:$topic:$groupid"

  // 根据 key 获取offsets
  def getOffsetsFromRedis(topics: Array[String], groupId: String): Map[TopicPartition, Long] = {
    val jedis: Jedis = getRedisConnection

    val offsets: Array[mutable.Map[TopicPartition, Long]] = topics.map { topic =>
      val key = getKey(topic, groupId)

      import scala.collection.JavaConverters._

      jedis.hgetAll(key)
        .asScala
        .map { case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong }
    }

    // 归还资源
    jedis.close()

    offsets.flatten.toMap
  }

  // 将offsets保存到Redis中
  def saveOffsetsToRedis(offsets: Array[OffsetRange], groupId: String): Unit = {
    // 获取连接
    val jedis: Jedis = getRedisConnection

    // 组织数据
    offsets.map{range => (range.topic, (range.partition.toString, range.untilOffset.toString))}
        .groupBy(_._1)
      .foreach{case (topic, buffer) =>
        val key: String = getKey(topic, groupId)

        import scala.collection.JavaConverters._
        val maps: util.Map[String, String] = buffer.map(_._2).toMap.asJava

        // 保存数据
        jedis.hmset(key, maps)
      }

    jedis.close()
  }

  def main(args: Array[String]): Unit = {
    val topics = Array("topicB", "topicC")
    val groupid = "group01"
    val x: Map[TopicPartition, Long] = getOffsetsFromRedis(topics, groupid)
    x.foreach(println)
  }
}

// hmset kafka:topic:topicB:group01 0 0 1 100 2 200
// hgetall kafka:topic:topicB:group01

//    val groupId: String = "group01"
//    val topics: Array[String] = Array("topicB")
//    val offsets: Map[TopicPartition, Long] = getOffsetsFromRedis(topics, groupId)
//    offsets.foreach(println)

//    val groupid = "group01"
//    val topic = "topicB"
//    val offsets = Array(
//      OffsetRange(topic, 0,   0,  600),
//      OffsetRange(topic, 1, 100, 1000),
//      OffsetRange(topic, 2, 200, 2000)
//    )
//    saveOffsetsToRedis(offsets, groupid)

// ./redis-server /opt/lagou/software/redis-5.0.5/redis.conf