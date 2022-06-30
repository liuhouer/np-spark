package cn.lagou.tmp

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random

object SparkPi {
  def main(args: Array[String]): Unit = {
    // 1、创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 2、生成RDD;RDD转换
    val slices = if (args.length > 0) args(0).toInt else 10
    val N = 100000000
    val count = sc.makeRDD(1 to N, slices)
        .map(idx => {
          val (x, y) = (random, random)
          if (x*x + y*y <= 1) 1 else 0
        }).reduce(_+_)

    // 3、结果输出
    println(s"Pi is roughly ${4.0 * count / N}")

    // 4、关闭SparkContext
    sc.stop()
  }
}
