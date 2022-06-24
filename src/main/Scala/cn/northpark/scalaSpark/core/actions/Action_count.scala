package cn.northpark.scalaSpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * count 统计RDD共有多少行数据
  */
object Action_count {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("count")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/sampleData.txt")
    val result: Long = lines.count()
    println(result)
    sc.stop()
  }
}
