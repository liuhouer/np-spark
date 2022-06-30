package cn.lagou.tmp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FindFriends {
  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 生成RDD
    val lines: RDD[String] = sc.textFile("file:///C:\\Project\\LagouBigData\\data\\fields.dat")

    // 方法一：核心思想笛卡尔积，然后做数据过滤和变形
    val friendRDD = lines.map(line => {
      val value = line.split(",")
      val userID = value(0).trim
      val friends = value(1).trim.split("\\s+")
      (userID, friends)
    })

    val commonFriendsRDD = friendRDD.cartesian(friendRDD)
      .filter{case ((user1, _), (user2, _)) => user1 < user2}
      .map{case ((user1, friends1),(user2, friends2)) => ((user1,user2), friends1.toSet & friends2.toSet)}
      .sortByKey()
      .collect().foreach(println)

    // 方法二：高效的方法
    // 这里的 flatMap 要注意；还有reduceByKey 不是普通的求和要注意
    lines.flatMap(line => {
      val arr = line.split(",")
      val friends = arr(1).trim.split(" ")
      friends.combinations(2).map(x => ((x(0), x(1)), Set(arr(0))))
    }).reduceByKey(_ | _)
      .sortByKey()
      .collect.foreach(println)

    // 关闭 SparkContext
    sc.stop()
  }
}
