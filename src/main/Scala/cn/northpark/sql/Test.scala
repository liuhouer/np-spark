package cn.northpark.sql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
//    val df: DataFrame = spark.read.json("./data/json")
    val df = spark.read.format("json").load("./data/json")
    df.show(100)
    df.printSchema()

    //    val rows: Array[Row] = df.take(10)
        //select name ,age from table
//    val frame: DataFrame = df.select(df.col("name"),df.col("age"))
//    frame.show()
//    rows.foreach(println)
//    import spark.implicits._
//    val frame: DataFrame = df.select($"name".equalTo("zhangsan"),$"age")

//    frame.show()
    //select name ,age from table where age>18
    df.createOrReplaceTempView("aaa")
    df.createOrReplaceGlobalTempView("bbb")
    val frame: DataFrame = spark.sql("select name,age from aaa where age>18")
    frame.show()

    val sparkSession1: SparkSession = spark.newSession()

    sparkSession1.sql("select name,age from global_temp.bbb where age>18").show()
  }
}
