package basic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object RDDQueueDStream {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName("RDDQueueStream").setMaster("local[2]")
    // 每隔5秒对数据进行处理
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val rddQueue = new Queue[RDD[Int]]()
    val queueStream = ssc.queueStream(rddQueue)
    val mappedStream = queueStream.map(r => (r % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()
    ssc.start()

    // 每秒产生一个RDD
    for (i <- 1 to 5){
      rddQueue.synchronized {
        val range = (1 to 100).map(_*i)
        rddQueue += ssc.sparkContext.makeRDD(range, 2)
      }
      Thread.sleep(2000)
    }
    ssc.stop()
  }
}
