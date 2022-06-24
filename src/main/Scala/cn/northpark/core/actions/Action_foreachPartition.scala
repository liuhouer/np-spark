package cn.northpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object Action_foreachPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("foreachPartition")
    val sc = new SparkContext(conf)
    val infos = sc.parallelize(List[String]("a","b","c","d","e","f","g"),4)
//    infos.foreachAsync()
  }
}
