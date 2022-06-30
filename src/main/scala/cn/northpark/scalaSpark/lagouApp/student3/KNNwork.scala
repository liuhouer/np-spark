package cn.northpark.scalaSpark.lagouApp.student3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KNNwork {

  /**
   * 题目：使用鸢尾花数据集实现KNN算法
   */
  private val K = 13
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KNNwork").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val testRdd: RDD[(String, Array[Double])] = sc.textFile("src\\data\\Iris.csv").map(line => {
      val fields: Array[String] = line.split(",")
      (fields.last, fields.init.tail.map(_.toDouble))
    })
    // 将testRdd广播，避免shuffer
    val testBC: Broadcast[Array[(String, Array[Double])]] = sc.broadcast(testRdd.collect())
    val varRdd: RDD[(String, Array[Double])] = sc.textFile("src\\data\\IrisB.csv").map(line => {
      val fields: Array[String] = line.split(",")
      (fields.last, fields.init.tail.map(_.toDouble))
    })
    val varData: Array[(String, Array[Double])] = varRdd.collect()
    varData.foreach(elem => {
      val res: Array[(Double, String)] = testBC.value.map(point => (getDistance(point._2, elem._2), point._1))
      val tuples: Array[(Double, String)] = res.sortBy(_._1).take(K)
      val labels: Array[String] = tuples.map(_._2)
      print(s"${elem._2.toBuffer} : ")
      labels.groupBy(x => x).mapValues(_.length).foreach(print)
      println()
    })
    sc.stop()
  }

  /** 求距离 */
  def getDistance(x: Array[Double], y: Array[Double]): Double = {
    math.sqrt(x.zip(y).map(z => math.pow(z._1 - z._2, 2)).sum)
  }
}
