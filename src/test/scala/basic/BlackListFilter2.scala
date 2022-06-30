package basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BlackListFilter2 {
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
    clickStreamFormatted.transform{clickRDD =>
      val spark = SparkSession
        .builder()
        .config(rdd.sparkContext.getConf)
        .getOrCreate()

      import spark.implicits._
      val clickDF: DataFrame = clickRDD.toDF("word", "line")
      val blackDF: DataFrame = blackListRDD.toDF("word", "flag")
      clickDF.join(blackDF, Seq("word"), "left")
        .filter($"flag".isNull)
        .select("line")
        .rdd
    }.print()

    // 启动流式作业
    ssc.start()
    ssc.awaitTermination()
  }
}
