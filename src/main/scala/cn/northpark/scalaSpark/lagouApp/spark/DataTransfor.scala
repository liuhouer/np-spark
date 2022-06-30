package cn.northpark.scalaSpark.lagouApp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


case class Info(id: String, start: String, end: String)

object DataTransfor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    import spark.implicits._

    // 准备数据
    val arr = Array("1 2019-03-04 2020-02-03", "2 2020-04-05 2020-08-04", "3 2019-10-09 2020-06-11")
    val rdd: RDD[Info] = spark.sparkContext.makeRDD(arr)
      .map { line =>
        val fields: Array[String] = line.split("\\s+")
        Info(fields(0), fields(1),fields(2))
      }
    val ds: Dataset[Info] = spark.createDataset(rdd)
    ds.createOrReplaceTempView("t1")
    ds.show

    // 查询到start 列
    val df1 = spark.sql(
      """
        |select start
        |  from t1
        |""".stripMargin
    )
    // 查询到end 列
   val df2 =  spark.sql(
      """
        |select end
        |  from t1
        |""".stripMargin
    )

    // 两个dateFrame 进行合并 并按照开始时间排序
     val ds2 = df1.union(df2).orderBy("start")
    ds2.show()
    ds2.createOrReplaceTempView("t2")

    // 进行窗体滑动，取后一个填充当前行，没有下一行则用当前行数据
    val df3 = spark.sql(
      """
        | select start,
        |    lead(start,1,start) over (order by start) as lag_time
        |     from t2
        |""".stripMargin
    )

    // 展示转换后到表数据
   df3.show()

    spark.close()
  }
}
