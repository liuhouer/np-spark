package cn.northpark.scalaSpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * takeSample(withReplacement,num,seed)
  * 随机抽样将数据结果拿回Driver端使用，返回Array。
  * withReplacement:有无放回抽样
  * num:抽样的条数
  * num:种子
  */
object Action_takeSample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("takeSample")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/words")
    val result: Array[String] = lines.takeSample(false, 300, 100)
    result.foreach(println)
  }
}
