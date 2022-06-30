package cn.northpark.scalaSpark.lagouApp.advanced

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 两阶段聚合解决数据倾斜
object DataSkew2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 本地文件是32M一个块；文件大小为88M，如果不做重分区那么将有3个数据块
    val rdd1: RDD[String] = sc.textFile("data/log1.txt").coalesce(2)
    println(s"rdd1.Partitions = ${rdd1.getNumPartitions}")

    val rdd2 = rdd1.flatMap(_.split("\\s+"))
      .map((_, 1))
      .groupByKey()
      .map{case (k, v) => (k, v.sum)}
    println(s"rdd2.Partitions = ${rdd2.getNumPartitions}")
    rdd2.collect.foreach(println)

    // 可以发现有数据倾斜
    val info = getRDDPartitionCount(rdd2)
    println(info)

    // 使用两阶段聚合，解决数据倾斜问题
    // 加前缀，执行局部聚合；去前缀，执行全局聚合
    val random = scala.util.Random
    val rdd3: RDD[(String, Int)] = rdd1.filter(_.trim.size > 0)
      .flatMap(_.split("\\s+"))
      .map { word =>
        val prefix = random.nextInt(5)
        (s"${prefix}_$word", 1)
      }.groupByKey()
      .map { case (k, v) =>
        val word: String = k.split("_")(1)
        (word, v.sum)
      }.groupByKey()
      .mapValues(v => v.sum)
    rdd3.collect().foreach(println)

    println("end!")
    Thread.sleep(100000000)
    sc.stop()
  }

  def getRDDPartitionCount(rdd: RDD[_]): String = {
    rdd.mapPartitionsWithIndex{case (idx, iter) =>
      Iterator(s"$idx : ${iter.size}")
    }.collect().mkString("\n")
  }
}