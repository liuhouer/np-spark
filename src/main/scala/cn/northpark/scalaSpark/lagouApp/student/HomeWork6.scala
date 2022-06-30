package cn.northpark.scalaSpark.lagouApp.student

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object HomeWork6 {
  def main(args: Array[String]): Unit = {
    // 初始化,框架代码
    val spark = SparkSession
      .builder()
      .appName("HomeWork6")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    // 设置日志级别
    sc.setLogLevel("warn")

    // 养成习惯导入
    import org.apache.spark.sql.functions._
    import spark.implicits._
    // 初始数据导入,这里是集合变df
    val df: DataFrame = List("1 2019-03-04 2020-02-03", "2 2020-04-05 2020-08-04", "3 2019-10-09 2020-06-11").toDF()

    // DSL方式
    // 窗口操作
    val w1: WindowSpec = Window.orderBy($"value" asc).rowsBetween(0, 1)
    df.as[String]
      // 去除前面序号
      .map(str => str.split(" ")(1) + " " + str.split(" ")(2))
      // 开始结束时间切分
      .flatMap(str => str.split("\\s+"))
      // 去重
      .distinct()
      // 这里是所有时间的排序
      .sort($"value" asc)
      // 每两列输出一行
      .withColumn("new", max("value") over (w1))
      .show()

    // SQL方式
    df.flatMap{ case Row(line: String) =>
      line.split("\\s+").tail
    }.toDF("date")
      .createOrReplaceTempView("t1")

    spark.sql(
      """
        |select date, max(date) over (order by date rows between current row and 1 following) as date1
        |  from t1
        |""".stripMargin).show

    spark.close()
  }
}
