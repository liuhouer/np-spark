package cn.northpark.scalaSpark.lagouApp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object KNN {
  def distance(a:Array[Double],b:Array[Double]):Double={
      val distant = math.sqrt(math.pow(a(0)-b(0),2)+math.pow(a(1)-b(1),2)+math.pow(a(2)-b(2),2)+math.pow(a(3)-b(3),2))
      distant
  }

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val K = 5

    val trainRDD:RDD[(Double,Double,Double,Double,String)] = sc.textFile("src\\data\\knn.txt").map({
      line=>
        val fields = line.split(",")
        (fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble,fields(5))
    })

    val testRDD:RDD[(Double,Double,Double,Double)] = sc.textFile("src\\data\\knn_check.txt").map({
      line=>
        val fields = line.split(",")
        (fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble)
    })

    val bcTrainRDD = sc.broadcast(trainRDD.collect())
    val bcK = sc.broadcast(K)

    val resultRDD = testRDD.map({
       data=>
         val testArr = Array(data._1,data._2,data._3,data._4)
         val trains = bcTrainRDD.value
         val set = ArrayBuffer.empty[(Double,String)]
        trains.foreach(t=>{
          val trainArr = Array(t._1,t._2,t._3,t._4)
          val distant = distance(testArr,trainArr)
          set.+=((distant,t._5))
        })

        val list = set.sortBy(_._1)
        //统计每个分类等数量 Map
        var categoryMap = Map.empty[String,Int]
        val k = bcK.value
        for (i<-0 until k){
          val category = list(i)._2
          val count = categoryMap.getOrElse(category,0)+1
          categoryMap += (category -> count)
        }
        val (category,count) = categoryMap.maxBy(_._2)
         (data._1,data._2,data._3,data._4,category)
    })

//    resultRDD.repartition(1).saveAsTextFile("src\\data\\knn_result1.txt")
      resultRDD.collect().foreach(println)

    // 关闭SparkContext
    sc.stop()
  }
}
