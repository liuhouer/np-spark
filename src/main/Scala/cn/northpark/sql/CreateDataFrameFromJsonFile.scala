package cn.northpark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CreateDataFrameFromJsonFile {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    val dataset :Dataset[Row] = spark.read.json("./data/json")
    dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)

    dataset/*.map(row=>{row})*/.count()
    while(true){

    }




//    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
//    val df :Dataset[Row] = spark.read.json("./data/json")
//    val df = frame
//    val rdd: RDD[Row] = df.rdd
//    rdd.map(line=>{
////      val  list1 = List[String](line.getAs("name"))
////      val  list2 = List[Long](line.getAs("age"))
//      df.show()
//
//
//      val name = line.getAs("name").toString
//      var age = ""
//      if(line.getAs("age")!=null){
//        age = line.getAs("age").toString
//      }
//      println("name = "+name+",age = "+age)
//      line
//    }).collect()
////    df.show()
  }
}
