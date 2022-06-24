package cn.northpark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("streamingTest")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Durations.seconds(5))
//    val ssc = new StreamingContext(sc,Durations.seconds(5))
    ssc.sparkContext.setLogLevel("Error")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node5",9999)
    val words: DStream[String] = lines.flatMap(one=>{one.split(" ")})
    val pairWords: DStream[(String, Int)] = words.map(one=>{(one,1)})
    val result: DStream[(String, Int)] = pairWords.reduceByKey((v1:Int, v2:Int)=>{v1+v2})
//    result.print(100)

    result.foreachRDD(pairRDD=>{
      println("===========Driver 广播黑名单=============")

      pairRDD.filter(one => {
        println("executor in filter ======="+one)
        true

      }).count()
//      val resultRDD: RDD[(String, Int)] = newRDD.map(one => {
//        println("executor in map *****" + one)
//        one
//      })
//      resultRDD.count()
      
    })


    ssc.start()
    ssc.awaitTermination()

    ssc.stop(false)
  }
}
