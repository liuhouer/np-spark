package cn.northpark.core.examples.pvAnduv

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object PvAndUv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("pvuv")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/pvuvdata")
    //pv
    lines.map(one=>{(one.split("\t")(5),1)}).reduceByKey((v1,v2)=>{v1+v2}).sortBy(tp=>{tp._2},false).foreach(println)
    //uv
    val distinctTrans = lines.map(one=>{s"${one.split("\t")(0)}_${one.split("\t")(5)}"}).distinct()
    distinctTrans.map(one=>{(one.split("_")(1),1)}).reduceByKey(_+_).sortBy(_._2).foreach(println)
    //计算每个网址 最活跃的网址及对应的人数
    val groupTrans = lines.map(one=>{(one.split("\t")(5),one.split("\t")(1))}).groupByKey()
    groupTrans.map(tp=>{
      val site = tp._1
      val localIterator = tp._2.iterator
      val localMap = mutable.Map[String,Int]()
      while(localIterator.hasNext){
        val currentLocal = localIterator.next()
        if(localMap.contains(currentLocal)){
          val count = localMap.get(currentLocal).get+1
          localMap.put(currentLocal,count)
        }else{
          localMap.put(currentLocal,1)
        }
      }

      //对Map 排序
      val newList: List[(String, Int)] = localMap.toList.sortBy(tp=>{-tp._2})
      (site,newList.toBuffer)

    }).foreach(println)
  }
}
