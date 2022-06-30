package basic

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackListFilter1 {
  def main(args: Array[String]) {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("WARN")

    // 黑名单数据
    val blackList = Array(("spark", true), ("scala", true))
    val blackListRDD = ssc.sparkContext.makeRDD(blackList)

    // 生成测试DStream。使用ConstantInputDStream
    val strArray: Array[String] = "spark java scala hadoop kafka hive hbase zookeeper"
      .split("\\s+")
      .zipWithIndex
      .map { case (word, idx) => s"$idx $word" }
    val rdd = ssc.sparkContext.makeRDD(strArray)
    val clickStream = new ConstantInputDStream(ssc, rdd)

    // 流式数据的处理
    val clickStreamFormatted = clickStream.map(value => (value.split(" ")(1), value))
    clickStreamFormatted.transform(clickRDD => {
      // 通过leftOuterJoin操作既保留了左侧RDD的所有内容，又获得了内容是否在黑名单中
      val joinedBlackListRDD: RDD[(String, (String, Option[Boolean]))] = clickRDD.leftOuterJoin(blackListRDD)

      joinedBlackListRDD.filter { case (word, (streamingLine, flag)) =>
        if (flag.getOrElse(false)) false
        else true
      }.map { case (word, (streamingLine, flag)) => streamingLine }
    }).print()

    // 启动流式作业
    ssc.start()
    ssc.awaitTermination()
  }
}
