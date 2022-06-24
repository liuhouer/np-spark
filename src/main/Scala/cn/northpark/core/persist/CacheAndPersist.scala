package cn.northpark.core.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * cache()和persist()注意问题：
  * 1.cache()和persist()持久化单位是partition,cache()和persist()是懒执行算子，需要action算子触发执行。
  * 2.对一个RDD使用cache或者persist之后可以赋值给一个变量，下次直接使用这个变量就是使用的持久化的数据。也可以直接对RDD进行cache或者persist不赋值给一个变量
  * 3.如果采用第二种方式赋值给变量的话，后面不能紧跟action算子。
  * 4.cache()和persist()的数据在当前application执行完成之后会自动清除。
  */
object CacheAndPersist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("cacheAndPersist")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    var lines = sc.textFile("./data/persistData.txt")
    
//    lines.cache()
    lines = lines.persist(StorageLevel.MEMORY_ONLY)
    val startTime1 = System.currentTimeMillis()
    val count1 = lines.count()
    val endTime1 = System.currentTimeMillis()
    println("count1 = "+count1+",time = "+(endTime1-startTime1)+"ms")

    val startTime2 = System.currentTimeMillis()
    val count2 = lines.count()
    val endTime2 = System.currentTimeMillis()
    println("count2 = "+count2+",time = "+(endTime2-startTime2)+"ms")

    sc.stop()
  }
}
