package cn.northpark.scalaSpark.core.examples

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义累加器，实现姓名的个数相加，年龄相加
  */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("accumulator")
    val sc = new SparkContext(conf)

    val accumulator: LongAccumulator = sc.longAccumulator("My Accumulator")
    val lines = sc.textFile("./data/words")
    val trans = lines.map(line=>{
      accumulator.add(1)
      line
    })
    trans.collect()

    println(s"accumulator value is ${accumulator.value}")



  }
}
