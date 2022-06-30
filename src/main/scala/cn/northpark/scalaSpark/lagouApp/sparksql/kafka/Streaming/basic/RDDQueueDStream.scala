package cn.northpark.scalaSpark.lagouApp.sparksql.kafka.Streaming.basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable.Queue

object RDDQueueDStream {
  def main(args: Array[String]): Unit = {
    // 初始化
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    // 创建DStream
    val queue = new Queue[RDD[Int]]()
    val queueDStream: InputDStream[Int] = ssc.queueStream(queue)

    val result: DStream[(Int, Int)] = queueDStream.map(elem => (elem % 10, 1))
      .reduceByKey(_ + _)
    result.print()

    ssc.start()

    // 每秒生成一个RDD，将RDD放置在队列中
    for (i <- 1 to 5) {
      queue.synchronized{
        val range = (1 to 100).map(_*i)
        queue += ssc.sparkContext.makeRDD(range, 2)
      }
      Thread.sleep(1000)
    }
    ssc.stop()
  }
}