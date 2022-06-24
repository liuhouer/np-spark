package cn.northpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * foreach 遍历RDD中的每个元素
  */
object Action_foreach {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("foreach").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/words")
    lines.foreach(println)

  }
}
