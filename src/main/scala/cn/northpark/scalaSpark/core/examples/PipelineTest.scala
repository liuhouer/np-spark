package cn.northpark.scalaSpark.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * pipeline 测试
  *
  * Spark pipeline管道运行模式
  */
object PipelineTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("pipelineTest")
    val sc = new SparkContext(conf)
    val names:RDD[String] = sc.parallelize(Array[String]("zhangsan","lisi","wangwu"))
    val trans1: RDD[String] = names.filter(s => {
      println("*****filter*****" + s)
      true
    })
    val trans2: RDD[String] = trans1.map(s=>{
      println("######## map ##########"+s)
      s+"#"
    })
    trans2.foreach(println)
    sc.stop()
  }
}
