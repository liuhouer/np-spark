package cn.lagou.tmp

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.immutable

class MyPartitioner(n: Int) extends Partitioner{
  override def numPartitions: Int = n

  override def getPartition(key: Any): Int = {
    val k = key.toString.toInt
    k / 100
  }
}

object UserDefinedPartitioner {
  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 业务逻辑
    val random = scala.util.Random
    val arr: immutable.IndexedSeq[Int] = (1 to 100).map(idx => random.nextInt(1000))
    val rdd1: RDD[(Int, Int)] = sc.makeRDD(arr).map((_, 1))
    rdd1.glom.collect.foreach(x => println(x.toBuffer))

    println("********************************************************************************")

    val rdd2 = rdd1.partitionBy(new MyPartitioner(10))
    rdd2.glom.collect.foreach(x => println(x.toBuffer))

    // 关闭SparkContext
    sc.stop()
  }
}