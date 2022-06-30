package cn.northpark.scalaSpark.lagouApp.student2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.math.{pow, sqrt}

object work4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val sampleDF: Array[(Any, (Any, Any, Any, Any, Any))] = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv("data\\people1.csv")
      .rdd
      .collect()
      .map{
        x=>(x(5),(x(0),x(1),x(2),x(3),x(4)))
      }

    val testDF: Array[(Any, Any, Any, Any, Any)] = spark.read
      .option("header", true)
      .option("inferschema", true)
      .csv("data\\people2.csv")
      .rdd
      .collect()
      .map{
        x=>(x(0),x(1),x(2),x(3),x(4))
      }

    val N=9;

    testDF.flatMap { lineTest =>
      val Ndicts: Array[(Any, Int)] = sampleDF.map { lineSample =>
        (sqrt(pow(lineTest._1.toString.toDouble - lineSample._2._1.toString.toDouble, 2) +
          pow(lineTest._2.toString.toDouble - lineSample._2._2.toString.toDouble, 2) +
          pow(lineTest._3.toString.toDouble - lineSample._2._3.toString.toDouble, 2) +
          pow(lineTest._4.toString.toDouble - lineSample._2._4.toString.toDouble, 2) +
          pow(lineTest._5.toString.toDouble - lineSample._2._5.toString.toDouble, 2)
        ), lineSample._1)
      }.sortBy(_._1)
        .take(N)
        .map { x => (x._2, 1) }

      import spark.implicits._
      //对这K个点的label做wordcount，取最大值作为最终结果
      val tuple1 = sc.makeRDD(Ndicts).reduceByKey(_ + _).sortBy(_._2, false).first()

      println(s"样本${tuple1._2} 预测值： ${tuple1._1}  实际值 ： ${tuple1._1}")
      Ndicts
    }
    sc.stop()
  }
}
