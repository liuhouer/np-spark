package cn.lagou.tmp

import org.apache.spark.sql.SparkSession

object InputOutputDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val df1 = spark.read.format("parquet").load("data/users.parquet")
        // parquet是sparksql默认的数据源
        val df2 = spark.read.load("data/users.parquet")
        df1.show
        df2.show

        // Use CSV
        val df3 = spark.read.format("csv")
          .option("inferSchema", "true")
          .option("header", "true")
          .load("data/people1.csv")
        df3.show

        // Use JSON
        val df4 = spark.read.format("json")
          .load("data/emp.json")
          .show

        spark.sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW users
            |USING parquet
            |OPTIONS (path "data/users.parquet")
            |""".stripMargin
        )

        // parquet
        val df5 = spark.sql("select * from users")
        df5.show
        df5.write.format("parquet")
          .mode("overwrite")
          .option("compression", "snappy")
          .save("data/parquet")

        // JSON
        val fileJson = "data/emp.json"
        val df6 = spark.read.format("json").load(fileJson)

        spark.sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW emp
            | USING json
            | options(path "data/emp.json")
            |""".stripMargin)

        spark.sql("SELECT * FROM emp").show()
        spark.sql("SELECT * FROM emp").write
          .format("json")
          .mode("overwrite")
          .save("data/json")

        // CSV
        val fileCSV = "data/people1.csv"
        val df = spark.read.format("csv")
          .option("header", "true")
          .option("inferschema", "true")
          .load(fileCSV)

        spark.sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW people
            | USING csv
            |options(path "data/people1.csv",
            |        header "true",
            |        inferschema "true")
            |""".stripMargin)

        spark.sql("select * from people")
          .write
          .format("csv")
          .mode("overwrite")
          .save("data/csv")

    // JDBC
    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://linux123:3306/ebiz?useSSL=false")
      //&useUnicode=true
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "lagou_product_info")
      .option("user", "hive")
      .option("password", "12345678")
      .load()
    jdbcDF.show()

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://linux123:3306/ebiz?useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "lagou_product_info_back")
      .option("user", "hive")
      .option("password", "12345678")
      .save()

    spark.close()
  }
}
