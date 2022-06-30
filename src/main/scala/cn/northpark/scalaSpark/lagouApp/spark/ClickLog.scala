package cn.northpark.scalaSpark.lagouApp.spark

import java.net.URI

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, rdd}


object ClickLog {
  def getADid(log:String): String ={
    val fields = log.split("\\s+")
    val uri = new URI(fields(3))
    val params = uri.getQuery.split("&")(4).split("=")
    params(1)
  }
  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //点击数
    //val lines = sc.textFile("data/click.log")
    val logRDD:RDD[(Int,Int)] = sc.textFile("src\\data\\click.log").map(line=>{
        val adid = getADid(line)
       (adid.toInt,1)
    }).reduceByKey(_+_)

    //曝光数
    //val imp = sc.textFile("data/imp.log")
    val impRDD:RDD[(Int,Int)] = sc.textFile("src\\data\\imp.log").map(line=>{
      val adid = getADid(line)
      (adid.toInt,1)
    }).reduceByKey(_+_)


    // 全联接 点击和曝光 RDD
    val joinRDD:RDD[(Int,(Int,Int))] = logRDD.fullOuterJoin(impRDD).map{case (adid,(click,imp))=>{
      (adid,(click.getOrElse(0),imp.getOrElse(0)))
    }}


    //joinRDD.saveAsTextFile("hdfs://node51:/user/root/data")
    joinRDD.foreach(println)

    Thread.sleep(1000000)

    // 关闭SparkContext
    sc.stop()


    /**
     * 构造点击RDD 使用reduceByKey 统计 无shuffle
     * 构造曝光RDD 使用reduceByKey 统计 无shuffle
     * 合并 点击数和曝光数使用了 全外连接 有shuffle
     * */



  }
}
