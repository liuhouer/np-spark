package cn.northpark.scalaSpark.lagouApp.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 查找IP地址归属地
 *
 * */
object IPZone {
  //IP地址转成长整型
  def ip2Lone(ip: String):Long = {
    val fields = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until  fields.length){
      ipNum = fields(i).toLong | ipNum << 8L
    }
    ipNum
  }
  // 二分查找IP地址对应地名
  def search(ip:Long,ipArr:Array[(Long,Long,String)]):Int={
    var start = 0
    var end = ipArr.length - 1
    while (start <= end){
      val mid = (start + end) >> 1
      val midKey = ipArr(mid)
      val midKeyStart = midKey._1
      val midKeyEnd = midKey._2
      if (ip >= midKeyStart && ip <= midKeyEnd){
        return mid
      }else if (ip < midKeyStart){
        end = mid -1
      }else{
        start = mid +1
      }
    }
    -1
  }
  def main(args: Array[String]): Unit = {
    //创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //读取数据
    val ipData: RDD[String] = sc.textFile("src\\data\\ip.dat")
    //将数据转换成(开始地址,结束地址,城市名称)
    val ipRule:RDD[(Long,Long,String)] = ipData.map({
      line=>
        val fields = line.split("[|]")
        val ipStart = fields(2).toLong
        val ipEnd = fields(3).toLong
        val province = fields(7)
        (ipStart,ipEnd,province)
    })
    //
    val ipRules = ipRule.collect()
    // 广播地址表
    val ipbc: Broadcast[Array[(Long,Long,String)]] = sc.broadcast(ipRules)
    // 读取日志数据
    val logData:RDD[String] = sc.textFile("src\\data\\http.log")
    // 将日志IP替换成地名 数据转换成(地址，1)形式
    val logFields:RDD[(String,Int)] = logData.map({
      line=>
        val logArr:Array[String] = line.split("[|]")
        val ip = ip2Lone(logArr(1))
        val ipArr:Array[(Long,Long,String)] = ipbc.value
        val idx = search(ip,ipArr)
        var province = "unknow"
        if (idx != -1){
          province = ipArr(idx)._3
        }

        (province,1)
    })
    // 按地名聚合累加并排序
    val res:RDD[(String,Int)] = logFields.reduceByKey(_+_).sortBy(-_._2)

    res.collect().foreach(println)

    // 关闭SparkContext
    sc.stop()

  }
}
