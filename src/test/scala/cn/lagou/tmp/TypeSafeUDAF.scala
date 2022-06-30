package cn.lagou.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}

case class Sales(id: Int, name1: String, sales: Double, discount: Double, name2: String, stime: String)
case class SalesBuffer(var sales2019: Double, var sales2020: Double)

class TypeSafeUDAF extends Aggregator[Sales, SalesBuffer, Double]{
  // 定义初值
  override def zero: SalesBuffer = SalesBuffer(0, 0)

  // 分区内数据合并
  override def reduce(buf: SalesBuffer, inputRow: Sales): SalesBuffer = {
    val sales: Double = inputRow.sales
    val year: String = inputRow.stime.take(4)
    year match {
      case "2019" => buf.sales2019 += sales
      case "2020" => buf.sales2020 += sales
      case _ => println("ERROR!")
    }
    buf
  }

  // 分区间数据合并
  override def merge(part1: SalesBuffer, part2: SalesBuffer): SalesBuffer = {
    val sales1 = part1.sales2019 + part2.sales2019
    val sales2 = part1.sales2020 + part2.sales2020
    SalesBuffer(sales1, sales2)
  }

  // 最终结果的计算
  override def finish(reduction: SalesBuffer): Double = {
    if (math.abs(reduction.sales2019) < 0.0000000001) 0
    else (reduction.sales2020 - reduction.sales2019) / reduction.sales2019
  }

  // 定义buffer 和 输出结果的编码器
  override def bufferEncoder: Encoder[SalesBuffer] = Encoders.product
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object TypeSafeUDAFTest{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
      .master("local[*]")
      .getOrCreate()

    val sales = Seq(
      Sales(1, "Widget Co",        1000.00, 0.00,    "AZ", "2019-01-02"),
      Sales(2, "Acme Widgets",     2000.00, 500.00,  "CA", "2019-02-01"),
      Sales(3, "Widgetry",         1000.00, 200.00,  "CA", "2020-01-11"),
      Sales(4, "Widgets R Us",     2000.00, 0.0,     "CA", "2020-02-19"),
      Sales(5, "Ye Olde Widgete",  3000.00, 0.0,     "MA", "2020-02-28"))

    import spark.implicits._
    val ds = spark.createDataset(sales)
    ds.show
    // name 会作为列名
    val rate: TypedColumn[Sales, Double] = new TypeSafeUDAF().toColumn.name("rate")
    ds.select(rate).show

    spark.stop()
  }
}