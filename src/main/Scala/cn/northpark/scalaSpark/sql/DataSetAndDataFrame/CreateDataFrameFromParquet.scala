package cn.northpark.scalaSpark.sql.DataSetAndDataFrame

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CreateDataFrameFromParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createdataframefromparquet").getOrCreate()
//    val df1: DataFrame = spark.read.json("./data/json")
    val df1: DataFrame = spark.read.format("json").load("./data/json")
    df1.show()

    /**
      * 保存成parquet文件
      */
    df1.write.mode(SaveMode.Append).format("parquet").save("./data/parquet")

    /**
      * 读取parquet文件
      *
      */
    val df2: DataFrame = spark.read.parquet("./data/parquet")
    val count: Long = df2.count()
    println(count)
    df2.show(100)
  }
}
