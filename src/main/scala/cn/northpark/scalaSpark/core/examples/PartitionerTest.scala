package cn.northpark.scalaSpark.core.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PartitionerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("partitionerTest")
    val sc = new SparkContext(conf)

    val rdd: RDD[(Int, String)] =
      sc.parallelize(List[(Int, String)]((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")), 2)
    rdd.mapPartitionsWithIndex((index,iter)=>{
      val list = new ListBuffer[(Int,String)]()
      while(iter.hasNext){
        val one = iter.next()
        println(s"rdd partition index = $index,value = $one")
        list.+=(one)
      }
      list.iterator
    },true).count()


    val newRDD: RDD[(Int, String)] = rdd.partitionBy(new Partitioner() {
      override def numPartitions: Int = 6

      override def getPartition(key: Any): Int = key.toString.toInt % numPartitions

    })
    newRDD.mapPartitionsWithIndex((index,iter)=>{
      val list = new ListBuffer[(Int,String)]()
      while(iter.hasNext){
        val one = iter.next()
        println(s"newRDD partition index = $index,value = $one")
        list.+=(one)
      }
      list.iterator
    },true).count()


  }
}
