package cn.northpark.scalaSpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将K,V格式的RDD回收到Driver端作为Map使用
  */
object Action_collectAsMap {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf()
    conf.setMaster("local").setAppName("collectAsMap")
    val sc = new SparkContext(conf)
    val weightInfos = sc.parallelize(List[(String,Double)](new Tuple2("zhangsan",78.4),new Tuple2("lisi",32.6),new Tuple2("wangwu",90.9)))
    val stringToDouble: collection.Map[String, Double] = weightInfos.collectAsMap()
    stringToDouble.foreach(tp=>{println(tp)})
    sc.stop()
  }
}
