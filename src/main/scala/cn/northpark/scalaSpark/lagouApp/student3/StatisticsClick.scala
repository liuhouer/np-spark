package cn.northpark.scalaSpark.lagouApp.student3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object StatisticsClick {

  /**
   * 用Spark-Core实现统计每个adid的曝光数与点击数，将结果输出到hdfs文件
   * 输出文件结构为adid、曝光数、点击数。注意：数据不能有丢失（存在某些adid有imp，没有clk；或有clk没有imp）
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StatisticsClick")
    val sc = new SparkContext(conf)
    val clickRdd: RDD[Array[String]] = sc.textFile("src\\data\\click.log").map(_.split("\\s+"))
    val impRdd: RDD[Array[String]] = sc.textFile("src\\data\\imp.log").map(_.split("\\s+"))
    val clickResRdd: RDD[(String, (Int, Int))] = clickRdd.map(array => {
      val adid: String = array(3).split("&")(2).split("=")(1)
      (adid, (1, 0))
    })
    val impResRdd: RDD[(String, (Int, Int))] = impRdd.map(array => {
      val adid: String = array(3).split("&")(2).split("=")(1)
      (adid, (0, 1))
    })
    val resRdd: RDD[(String, (Int, Int))] = clickResRdd.union(impResRdd).reduceByKey((x, y) => {
      (x._1 + y._1, x._2 + y._2)
    })
    resRdd.saveAsTextFile("src\\data\\res.dat")
  }
}
