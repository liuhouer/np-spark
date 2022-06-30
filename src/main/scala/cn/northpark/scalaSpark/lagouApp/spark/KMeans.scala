package cn.northpark.scalaSpark.lagouApp.spark

import org.apache.spark.{SparkConf, SparkContext}

object KMeans {
  // 计算两点之间等距离 的平方值
  def distance(a: (Double,Double,Double,Double),b:(Double,Double,Double,Double)):Double={
    math.pow(a._1 - b._1,2) + math.pow(a._2-b._2,2) + math.pow(a._3-b._3,2) + math.pow(a._4-b._4,2)
  }
  // 计算某个点到 一组中心点点距离，距离最近点中心点为当前点点分类，返回分类点所在集合下标
  def closestPoint(p: (Double,Double,Double,Double), points: Array[(Double,Double,Double,Double)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- points.indices) {
      val dist = distance(p,points(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
  }
  // 计算两个点的和
  def addPoints(p1: (Double,Double,Double,Double), p2: (Double,Double,Double,Double)) = {
    (p1._1 + p2._1, p1._2 + p2._2,p1._3 + p2._3,p1._4 + p2._4)
  }

  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 业务逻辑
    val points = sc.textFile("data/knn.txt")
      .map(line=>line.split(","))
      .map(fields=>((fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble),fields(5)))
      .persist()

    val K = 3
    //随机取样初始中心点
    val KPoints = points.takeSample(false,K,34)
    println("******************采样点******************")
    KPoints.foreach(println)

    // 每次迭代得到对新中心点到前一次中心点的距离
    var temp: Double = Double.PositiveInfinity

    // 误差阙值
    val minDist = 0.01
    // 新旧中心点距离 > minDist 一直迭代计算
    while (temp > minDist) {
       // 计算当前点与那个中心点距离最近，并将当前点归类到，最近到中心点
       val closest = points.map(point=>(closestPoint(point._1,KPoints.map{case (p,category)=>(p)}),(point,1)))

      // 根据角标，聚合周围最近的点，并把周围的点相加
      val pointStats = closest.reduceByKey{case ((point1,n1),(point2,n2)) => ((addPoints(point1._1,point2._1),point2._2),n1+n2) }

      // 计算新中心点
      val newPoints = pointStats.map{ case (i,(point,n)) => (i,((point._1._1/n,point._1._2/n,point._1._3/n,point._1._4/n),point._2))}.collectAsMap()


      temp = 0.0
      for (i <- 0 until K){
         temp += distance(KPoints(i)._1,newPoints(i)._1)
      }

      for (i <- 0 until K) {
         KPoints(i) = newPoints(i)
      }
    }

    println("************训练结果模型**************")
    KPoints.foreach(println)

    //加载检测数据
    val testRDD = sc.textFile("data/knn_check.txt").map(line=>{
        val fields = line.split(",")
      (fields(0),fields(1).toDouble,fields(2).toDouble,fields(3).toDouble,fields(4).toDouble)
    }).cache()

    println("***********检测结果****************")
    // 使用模型对检测数据分类
    testRDD.map(d=>{
       val idx = closestPoint((d._2,d._3,d._4,d._5),KPoints.map{case (p,category)=>(p)})
       val category = KPoints(idx)._2
      (d._1.toInt,(d._2,d._3,d._4,d._5,category))
    }).collect().sortBy(_._1).foreach(println)

    // 关闭SparkContext
    sc.stop()
  }
}
