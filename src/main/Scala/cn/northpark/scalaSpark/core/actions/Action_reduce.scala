package cn.northpark.scalaSpark.core.actions

import org.apache.spark.{SparkConf, SparkContext}

object Action_reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("countByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Array[Int](1,2,3,4,5))
    val result: Int = rdd.reduce((v1, v2) => {
      v1 + v2
    })
    println(result)


  }
}
