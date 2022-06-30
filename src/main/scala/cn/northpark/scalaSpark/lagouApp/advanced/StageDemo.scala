//package cn.northpark.scalaSpark.lagouApp.advanced
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext}
//
//import scala.collection.mutable
//
//object StageDemo {
//  def main(args: Array[String]): Unit = {
//    // 1、创建SparkContext
//    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//
//    val rdd1: RDD[String] = sc.textFile("data/http.log")
//    val rdd2: RDD[String] = rdd1.flatMap(line => line.split("\\s+"))
//    val rdd3: RDD[(String, Int)] = rdd2.map(x => (x, 1))
//    val rdd4: RDD[(String, Int)] = rdd3.repartition(10)
//    val rdd5: RDD[(String, Int)] = rdd4.reduceByKey(_ + _)
//    println(rdd5.toDebugString)
//    println(rdd5.dependencies)
//
//    val rdd6: RDD[(String, Int)] = sc.textFile("data/http.log")
//      .flatMap(line => line.split("\\s+"))
//      .map(x => (x, 1))
//      .repartition(11)
//      .reduceByKey(_ + _)
//    println(rdd6.dependencies)
//
//    val rdd7: RDD[(String, (Int, Int))] = rdd5.join(rdd6)
//    println(rdd7.dependencies)
//
//    val result: mutable.HashSet[ShuffleDependency[_, _, _]] =
//      sc.dagScheduler.getShuffleDependencies(rdd5)
//    result.foreach(println)
//
//    sc.stop()
//  }
//}