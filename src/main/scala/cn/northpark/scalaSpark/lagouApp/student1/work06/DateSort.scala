package cn.northpark.scalaSpark.lagouApp.student1.work06

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object DateSort {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName(this.getClass.getSimpleName.init)
            .getOrCreate()
        import org.apache.spark.sql.functions._


        var df: DataFrame = spark.read
            .option("header", "false")
            .option("delimiter", " ")
            .csv("src/data/date.log")
            .toDF("id", "startDate", "endDate")

        df = df.select("startDate").union(df.select("endDate"))

        // SQL方式
        println("SQL方式：")
        df.createOrReplaceTempView("t1")
        spark.sql(
            """
              |select startDate, nvl(lead(startDate) over(partition by c1 order by startDate), startDate) as endDate
              |from(
              |(select '1' c1, startDate
              |from t1
              |)t2)
              |""".stripMargin).show

        // DSL方式
        println("DSL方式")
        val w1 = Window.orderBy("startDate").rowsBetween(0, 1)
        df.sort("startDate")
            .withColumn("endDate", max("startDate") over (w1))
            .show()
    }
}
