package cn.northpark.scalaSpark.lagouApp.student2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



object work3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("warn")


    //为了减少shuffle,在读取文件的时候，不做聚合，只解析数据，并把数据组织成（adid,（点击数，曝光数））这样的形式返回
    val clickRDD: RDD[String] = sc.textFile("src\\data\\click.log")
    val clickRDD2: RDD[(String, (Int, Int))] = clickRDD.map { line =>
      val lines: Array[String] = line.split("\\s+")
      val adid = lines(3).substring(lines(3).lastIndexOf("=") + 1)
      (adid, (1, 0))
    }
    val impRDD: RDD[String] = sc.textFile("src\\data\\imp.log")
    val impRDD2: RDD[(String, (Int, Int))] = impRDD.map { line =>
      val lines = line.split("\\s+")
      val adid = lines(3).substring(lines(3).lastIndexOf("=") + 1)
      (adid, (0, 1))
    }

    //将曝光数据和点击数据合并（此时没有shuffle）
    val result: RDD[(String, (Int, Int))] = clickRDD2.union(impRDD2)
      //按照adid分组求和（一次shuffle），得到最终结果
      .reduceByKey((x, y) => (x._1 + y._1, y._1 + y._2))
    result.foreach(println)

    //保存到hdfds
    //result.saveAsTextFile("hdfs://linux121:9000/data/")

    //保存到本地
    result.saveAsTextFile("data/ci")

    //    Thread.sleep(100000000)

    sc.stop()
  }
}
