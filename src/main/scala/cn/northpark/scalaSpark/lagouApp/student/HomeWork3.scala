package cn.northpark.scalaSpark.lagouApp.student

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HomeWork3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HomeWork3").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    val clickLog = sc.textFile("data/click.log")
    val impLog = sc.textFile("data/imp.log")

    // 读文件
    val clkRDD = clickLog.map { line =>
      val arr = line.split("\\s+")
      val adid = arr(3).substring(arr(3).lastIndexOf("=") + 1)
      (adid, (1, 0))
    }

    // 读文件
    val impRDD = impLog.map { line =>
      val arr = line.split("\\s+")
      val adid = arr(3).substring(arr(3).lastIndexOf("=") + 1)
      (adid, (0, 1))
    }

    // join
    val RDD: RDD[(String, (Int, Int))] = clkRDD.union(impRDD)
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // 写hdfs
//    RDD.saveAsTextFile("hdfs://linux121:9000/data/")

    sc.stop()
  }
}
