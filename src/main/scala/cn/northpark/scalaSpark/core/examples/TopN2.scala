package cn.northpark.scalaSpark.core.examples

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control._
/**
  * 分组取topN问题
  * 定义定长数组
  */
object TopN2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("topN")
    val sc = new SparkContext(conf)
    val infos = sc.textFile("./data/scores.txt")
    val pairInfo = infos.map(one=>{(one.split("\t")(0),one.split("\t")(1).toInt)})
    val result: Array[(String, mutable.Buffer[Int])] = pairInfo.groupByKey().map(tp => {
      val className = tp._1
      val iter = tp._2.iterator

      val top3Score = new Array[Int](3)
      val loop = new Breaks
      while (iter.hasNext) {
        val currScore = iter.next()
        loop.breakable {
          for (i <- 0 until top3Score.size) {
            if (top3Score(i) == 0) {
              top3Score(i) = currScore
              loop.break()
            } else if (currScore > top3Score(i)) {
              for (j <- 2 until(i, -1)) {
                top3Score(j) = top3Score(j - 1)
              }
              top3Score(i) = currScore
              loop.break()
            }
          }
        }
      }
      (className, top3Score.toBuffer)
    }).collect()
    result.foreach(println)
  }
}
