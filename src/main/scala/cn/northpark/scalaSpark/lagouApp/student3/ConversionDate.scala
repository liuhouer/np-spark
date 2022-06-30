package cn.northpark.scalaSpark.lagouApp.student3

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ConversionDate {

  /**
   * A表有三个字段：ID、startdate、enddate，有3条数据：
   * 1 2019-03-04 2020-02-03
   * 2 2020-04-05 2020-08-04
   * 3 2019-10-09 2020-06-11
   *写SQL（需要SQL和 DSL）将以上数据变化为：
   * 2019-03-04 2019-10-09
   * 2019-10-09 2020-02-03
   * 2020-02-03 2020-04-05
   * 2020-04-05 2020-06-11
   * 2020-06-11 2020-08-04
   * 2020-08-04 2020-08-04
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("ConversionDate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val seq1 = Seq(DateObject(1, "2019-03-04", "2020-02-03"), DateObject(2, "2020-04-05", "2020-08-04"), DateObject(3, "2019-10-09", "2020-06-11"))
    val ds: Dataset[DateObject] = spark.createDataset(seq1)
    ds.createOrReplaceTempView("dataTable")
    spark.sql(
      """
        |
        |with t2 as(
        |    select d1 from (
        |        select explode(array(startDate, endDate)) d1 from dataTable
        |    ) t1 order by d1
        |)
        |select d1, nvl(lead(d1) over(order by d1), d1) lead1 from t2
        |""".stripMargin
    ).show()
    spark.close()
  }
}

case class DateObject(id: Int, startDate: String, endDate: String)
