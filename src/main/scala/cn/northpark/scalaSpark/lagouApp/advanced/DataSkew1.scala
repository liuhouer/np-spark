package cn.northpark.scalaSpark.lagouApp.advanced

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DataSkew1 {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 定义数据
    val rdd: RDD[Int] = sc.makeRDD(1 to 30000000)
    val rdd1: RDD[(Int, Int)] = rdd.map(x => (if (x > 3000000) (x % 3000000) * 7 else x, 1))

     // 观察数据倾斜
//    rdd1.groupByKey().mapPartitionsWithIndex{ (index, iter) =>
//      Iterator(s"$index : ${iter.size}")
//    }.collect
//
//    rdd1.repartition(5).groupByKey().mapPartitionsWithIndex{ (index, iter) =>
//      Iterator(s"$index : ${iter.size}")
//    }.collect

    rdd1.groupByKey(3).mapPartitionsWithIndex{ (index, iter) =>
      Iterator(s"$index : ${iter.size}")
    }.collect


    println("OK!")
    Thread.sleep(1000000000)

    sc.stop()
  }
}
