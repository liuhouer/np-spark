package cn.northpark.scalaSpark.core.examples

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("broadCast")
    val sc = new SparkContext(conf)
    val list = List[String]("zhangsan","tianqi")
    val blackList: Broadcast[List[String]] = sc.broadcast(list)
    val nameRDD: RDD[String] = sc.parallelize(List[String]("zhangsan","lisi","wangwu","zhaoliu","tianqi"))
    nameRDD.filter(name=>{
      !blackList.value.contains(name)
    }).foreach(println)
  }
}
