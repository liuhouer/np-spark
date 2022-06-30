package basic

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackListFilter3 {
  def main(args: Array[String]) {
    // 初始化
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.sparkContext.setLogLevel("WARN")

    // 黑名单数据
    val blackList = Array(("spark", true), ("scala", true))
    val blackListBC: Broadcast[Array[String]] = ssc.sparkContext.broadcast(blackList.filter(_._2).map(_._1))

    // 生成测试DStream。使用ConstantInputDStream
    val strArray: Array[String] = "spark java scala hadoop kafka hive hbase zookeeper"
      .split("\\s+")
      .zipWithIndex
      .map { case (word, idx) => s"$idx $word" }
    val rdd = ssc.sparkContext.makeRDD(strArray)
    val clickStream = new ConstantInputDStream(ssc, rdd)

    // 流式数据的处理
    clickStream.map(value => (value.split(" ")(1), value))
        .filter{case (word, _) => !blackListBC.value.contains(word)}
        .map(_._2)
        .print()

    // 启动流式作业
    ssc.start()
    ssc.awaitTermination()
  }
}
