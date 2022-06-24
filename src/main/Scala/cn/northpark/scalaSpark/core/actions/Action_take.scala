package cn.northpark.scalaSpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取出RDD中的前N个元素
  */
object Action_take {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("take").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/words")
    val array = lines.take(3)
    array.foreach(println)
}
}
