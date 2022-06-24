package cn.northpark.sql.DataSetAndDataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 通过反射的方式将DataSet转换成DataFrame
  * 1.自动生成的DataFrame 会按照对象中的属性顺序显示
  */
object CreateDataFrameFromRDDWithReflection {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createDataFrameFromRDDWithReflection").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    /**
      * 直接读取文件为DataSet
      */
    //    val person: Dataset[String] = spark.read.textFile("./data/people.txt")
    //    val personDs: Dataset[Person] = person.map(one => {
    //      val arr = one.split(",")
    //      Person(arr(0).toInt, arr(1).toString, arr(2).toInt, arr(3).toDouble)
    //    })

    /**
      * 直接读取文件为RDD
      */
    val rdd: RDD[String] = spark.sparkContext.textFile("./data/people.txt")
    val personDs: RDD[Person] = rdd.map(one => {
      val arr = one.split(",")
      Person(arr(0).toInt, arr(1).toString, arr(2).toInt, arr(3).toDouble)
    })

    val frame: DataFrame = personDs.toDF()
    frame.show()
    /**
      * dataFrame api 操作
      */
    frame.createOrReplaceTempView("people")
    val teenagersDF: DataFrame = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.show()
    //根据row中的下标获取值
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    //根据row中的字段获取值
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()

    /**
      * 数据集[Map[K,V]没有预定义的编码器，在这里定义
      */
//    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
//    //Map 没有额外的编码器，在转换过程中Map 需要隐式转换的编码器
//    val result: Dataset[Map[String, Any]] = teenagersDF.map(teenager=>{teenager.getValuesMap[Any](List("name","age"))})
//    result.collect().foreach(println)


  }
}
