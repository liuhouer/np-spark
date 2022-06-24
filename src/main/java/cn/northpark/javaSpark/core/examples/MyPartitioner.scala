package cn.northpark.javaSpark.core.examples

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MyPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("partitionerTest")
    val sc = new SparkContext(conf)
    val nameRDD = sc.parallelize(List[(Int,String)]((1,"zhangsan"),(2,"lisi"),(3,"wangwu"),(4,"zaoliu"),(5,"tianqi"),(6,"tianba")),2)
    nameRDD.mapPartitionsWithIndex((index:Int,iter)=>{
      val list = new ListBuffer[(Int,String)]
      iter.foreach(one=>{
        list.append(one)
        println(s"partitionIndex = ${index} , value = ${one}")
      })
      list.iterator
    }).count()

    val resultRDD: RDD[(Int, String)] = nameRDD.partitionBy(new Partitioner() {
      override def numPartitions: Int = 3

      override def getPartition(key: Any): Int = {
        val currKey = key.toString.toInt
        return currKey % numPartitions
      }
    })
    val end: RDD[String] = resultRDD.mapPartitionsWithIndex((index, iter) => {
      val list = new ListBuffer[String]
      while (iter.hasNext) {
        list.append(s"partition Index = $index,value = ${iter.next()}")
      }
      list.iterator
    })
    end.collect().foreach(println)

    

  }
}
