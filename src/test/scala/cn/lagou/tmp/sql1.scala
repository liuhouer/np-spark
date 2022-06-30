package cn.lagou.tmp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object sql1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("aaa")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val arr = Seq((111, 1, 100),
      (111, 2, 0),
      (111, 3, 0),
      (111, 4, 0),
      (111, 5, 200),
      (111, 6, 0),
      (111, 7, 0),
      (111, 8, 0),
      (111, 9, 100),
    )

    val df: DataFrame = spark.createDataFrame(arr).toDF("id", "seq", "num")
    df.cache()

    df.createOrReplaceTempView("t1")
    spark.catalog.cacheTable("t1")
    spark.sql("select * from t1").show
    spark.sql("select id, seq, sum(num) over (partition by id order by seq) as num from t1").show

    val schema1 = StructType( StructField("name", StringType, false) ::
      StructField("age",  IntegerType, false) ::
      StructField("height", IntegerType, false) ::  Nil)

    spark.close()
  }
}
