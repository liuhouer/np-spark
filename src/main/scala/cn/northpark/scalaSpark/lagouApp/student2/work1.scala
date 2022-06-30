package cn.northpark.scalaSpark.lagouApp.student2

import org.apache.spark.sql.SparkSession

//定义ip样例类
case class IP(startIP:String ,endIp:String,address:String)

object work1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName.init)
      .getOrCreate()
    val sc = spark.sparkContext

    //解析http数据，得到IP信息
    val httpRDD = sc.textFile("data/http.log")
      .map(_.split("\\|")(1))

    //解析IP数据，得到归属地的IP范围
    val ipRDD = sc.textFile("data/ip.dat")
      .map{
        case line =>
          var lines: Array[String] = line.split("\\|")
          IP(lines(0),lines(1),lines(7))
      }

    import spark.implicits._
    //利用广播变量减少io，提高性能
    val brIP = sc.broadcast(ipRDD.map(x => (ip2Long(x.startIP), ip2Long(x.endIp), x.address)).collect())


    //关联ip和http
    httpRDD.map(x=>{
      var brIPValue: Array[(Long, Long, String)] = brIP.value
      var index = binarySearch(brIPValue,ip2Long(x));
      if (index != -1){
        brIPValue(index)._3
      }else{
        "null"
      }
    })
      .map(x=>(x,1))
      .reduceByKey(_+_)
      .collect().foreach(println)

    spark.close()
  }

  //转换为long行，方便比较ip的大小
  def ip2Long(ip:String): Long ={
    val ips = ip.split("[.]")
    var ipNum = 0L;
    for (i<-0 until ips.length){
      ipNum = ips(i).toLong|ipNum<<8L
    }
    ipNum

  }


  //二分法查找ip的归属地
  def binarySearch(ipAddres:Array[(Long,Long,String)],ip:Long): Int ={
    var low = 0;
    var high = ipAddres.length-1
    while (high>=low){
      val middle = (low+high)/2
      if((ip>=ipAddres(middle)._1)&&(ip<=ipAddres(middle)._2)){
        return middle
      }else if (ip<ipAddres(middle)._1){
        high =middle-1
      }else{
        low = middle+1
      }
    }
    -1
  }
}
