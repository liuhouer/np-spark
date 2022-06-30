package cn.lagou.tmp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SuperWordCount1 {
  val stopword = List("the", "a", "in", "and", "to", "of", "for", "is", "are", "on", "you", "can", "your", "as")
  val punctuation = """[,\\.?;:!"'()]"""

  def main(args: Array[String]): Unit = {
    // 1、创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 2、生成RDD
    val lines: RDD[String] = sc.textFile("file:///C:\\Project\\LagouBigData\\data\\swc.dat")

    // 3、RDD转换
    // 单词切分、转为小写
    val words: RDD[String] = lines.flatMap(_.split("\\s+"))
      .map(_.trim.toLowerCase())

    // 去停用词、标点
    val cleanWords: RDD[String] = words.filter(!stopword.contains(_))
      .map(_.replaceAll(punctuation, ""))

    // 计算词频，并按降序排序
    val result: RDD[(String, Int)] = cleanWords.map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    // 4、结果输出
    result.foreach(println)
    result.saveAsTextFile("file:///C:\\Project\\LagouBigData\\data\\wcoutput")

    // 5、关闭SparkContext
    sc.stop()
  }
}
