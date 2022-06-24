package cn.northpark.core.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计wordcount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/words")
    val words = lines.flatMap(line=>{
      line.split(" ")
    })
    val pairWords = words.map(word=>{
      new Tuple2(word,1)
    })
    val reduceResult = pairWords.reduceByKey((v1,v2)=>{v1+v2})
    val result = reduceResult.sortBy(tp=>{tp._2},false)
    result.foreach(println)
    sc.stop()
  }
}
