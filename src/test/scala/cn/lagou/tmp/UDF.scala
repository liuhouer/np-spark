package cn.lagou.tmp

import org.apache.spark.sql.{Row, SparkSession}

class UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val data = List(("scala", "author1"), ("spark", "author2"),
      ("hadoop", "author3"), ("hive", "author4"),
      ("strom", "author5"), ("kafka", "author6"))
    val df = spark.createDataFrame(data).toDF("title", "author")
    df.createTempView("books")

    // 定义函数并注册
    def len1(bookTitle: String):Int = bookTitle.length
    spark.udf.register("len1", len1 _)

    // UDF可以在select语句、where语句等多处使用
    spark.sql("select title, author, len1(title) from books").show
    spark.sql("select title, author from books where len1(title)>5").show

    // UDF可以在DataFrame、Dataset的API中使用
    import spark.implicits._
    df.filter("len1(title)>5").show

    // 不能通过编译
    // df.filter(len1($"title")>5).show
    // 能通过编译，但不能执行
    // df.select("len1(title)").show
    // 不能通过编译
    // df.select(len1($"title")).show

    // 如果要在DSL语法中使用$符号包裹字符串表示一个Column，需要用udf方法来接收函数。这种函数无需注册
    import org.apache.spark.sql.functions._
    val len2 = udf((bookTitle: String) => bookTitle.length)
    df.filter(len2($"title")>5).show
    df.select(len2($"title")).show

    // 不使用UDF
    df.map{case Row(title: String, author: String) => (title, author, title.length)}.show
    spark.stop()
  }
}
