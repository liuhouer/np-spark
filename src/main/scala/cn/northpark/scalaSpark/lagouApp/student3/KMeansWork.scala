package cn.northpark.scalaSpark.lagouApp.student3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object KMeansWork {

  /**
   * 题目：使用鸢尾花数据集实现KMeans算法
   */

  private val K = 3
  val conf: SparkConf = new SparkConf().setAppName("KMeansWork").setMaster("local[4]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")

    // 读文件
    val testRdd: RDD[(String, Array[Double])] = sc.textFile("src\\data\\IrisA.csv").map(line => {
      val fields: Array[String] = line.split(",")
      (fields.last, fields.init.tail.map(_.toDouble))
    })
    val nodes: Broadcast[Array[(String, Array[Double])]] = sc.broadcast(testRdd.collect())
    //        var centerNode: Array[Array[Double]] = getRandomData
    var centerNode = Array(Array(5.0,3.9,1.1,0.4),Array(7.1,2.0,2.3,2.4),Array(6.2,4.2,1.9,1.2))
    var flag = true
    var classifies: Array[(Int, (String, Array[Double]))] = null
    var n = 400
    while (n > 0) {
      classifies = classification(centerNode, nodes.value)
      val centerNodeNew: Array[(Int, Array[Double])] = calculateTheCenterPoint(classifies)
      // 对比中心点的方式，程序无法停止
      // if (comparaValue(centerNode, centerNodeNew)) flag = false
      n -= 1
    }
    classifies.foreach(println)
  }

  /** 比较两个新旧中心点是否相同 */
  def comparaValue(centerNode: Array[Array[Double]], centerNodeNew: Array[(Int, Array[Double])]) = {
    var flag = true
    for (elem <- centerNodeNew; if flag) {
      val index = elem._1
      val array = elem._2
      val node = centerNode(index)
      for (i <- node.indices; if flag) {
        if (math.abs(node(i) - array(i)) > 0.01) flag = false
      }
    }
    flag
  }

  /** 生成随机的三个点 */
  def getRandomData = {
    val random = new Random()
    val array = new Array[Array[Double]](3)
    for (i <- 0 until 3) {
      val tmp = new Array[Double](4)
      tmp(0) = random.nextInt(8)
      tmp(1) = random.nextInt(5)
      tmp(2) = random.nextInt(5)
      tmp(3) = random.nextInt(3)
      array(i) = tmp
    }
    array
  }

  /** 根据中心点分类 */
  def classification(centerNode: Array[Array[Double]], nodes: Array[(String, Array[Double])]) = {
    val res: Array[(Int, (String, Array[Double]))] = nodes.map(ns => {
      val node = ns._2
      var minValue = Integer.MAX_VALUE.toDouble
      var index = 0
      for (i <- centerNode.indices) {
        val distance: Double = getDistance(centerNode(i), node)
        if (distance < minValue) {
          minValue = distance
          index = i
        }
      }
      (index, (ns._1, ns._2))
    })
    res
  }

  /** 计算中心点 */
  def calculateTheCenterPoint(array: Array[(Int, (String, Array[Double]))]) = {
    val rdd: RDD[(Int, (String, Array[Double]))] = sc.makeRDD(array)
    val value: RDD[(Int, Array[Double])] = rdd.aggregateByKey((0.0, 0.0, 0.0, 0.0, 0))(
      (x, y) => (x._1 + y._2(0), x._2 + y._2(1), x._3 + y._2(2), x._4 + y._2(3), x._5 + 1),
      (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5)
    ).mapValues(x => {
      Array(x._1 / x._5, x._2 / x._5, x._3 / x._5, x._4 / x._5)
    })
    value.collect()
  }

  /** 计算两点之间距离 */
  def getDistance(x: Array[Double], y: Array[Double]): Double = {
    math.sqrt(x.zip(y).map(z => math.pow(z._1 - z._2, 2)).sum)
  }
}
