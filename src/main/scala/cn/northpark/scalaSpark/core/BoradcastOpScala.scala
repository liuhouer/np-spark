package cn.northpark.scalaSpark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：使用广播变量
 * Created by xuwei
 */
object BoradcastOpScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("BoradcastOpScala")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array(1,2,3,4,5))
    val varable = 2
    //dataRDD.map(_ * varable)
    //1：定义广播变量
    val varableBroadcast = sc.broadcast(varable)

    //2：使用广播变量，调用其value方法
    dataRDD.map(_ * varableBroadcast.value).foreach(println(_))

    sc.stop()
  }

}
