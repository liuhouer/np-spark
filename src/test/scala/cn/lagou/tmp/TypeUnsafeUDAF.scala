package cn.lagou.tmp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class TypeUnsafeUDAF extends UserDefinedAggregateFunction {
  // UDAF与DataFrame列有关的输入样式
  override def inputSchema: StructType = new StructType().add("sales", DoubleType).add("saledate", StringType)

  // 缓存中间结果
  override def bufferSchema: StructType = new StructType().add("year2019", DoubleType).add("year2020", DoubleType)

  // UDAF函数的返回值类型
  override def dataType: DataType = DoubleType

  // 布尔值，用以标记针对给定的一组输入，UDAF是否总是生成相同的结果。通常用true
  override def deterministic: Boolean = true

  // initialize就是对聚合运算中间结果的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
  }

  // UDAF的核心计算都发生在update函数中。
  // 扫描每行数据，都会调用一次update，输入buffer（缓存中间结果）、input（这一行的输入值）
  // update函数的第一个参数为bufferSchema中两个Field的索引，默认以0开始
  // update函数的第二个参数input: Row对应的是被inputSchema投影了的行。
  // 本例中每一个input就应该只有两个Field的值，input(0)代表销量，input(1)代表销售日期
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val salenumber = input.getAs[Double](0)
    // 输入的字符串为：2014-01-02，take(4) 取出有效的年份
    input.getString(1).take(4) match {
      case "2019" => buffer(0) = buffer.getAs[Double](0) + salenumber
      case "2020" => buffer(1) = buffer.getAs[Double](1) + salenumber
      case _ => println("ERROR!")
    }
  }

  // 合并两个分区的buffer1、buffer2，将最终结果保存在buffer1中
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  // 取出buffer（缓存的值）进行运算，得到最终结果
  override def evaluate(buffer: Row): Double = {
    println(s"evaluate : ${buffer.getDouble(0)}, ${buffer.getDouble(1)}")
    if (buffer.getDouble(0) == 0.0) 0.0
    else (buffer.getDouble(1) - buffer.getDouble(0)) / buffer.getDouble(0)
  }
}

object TypeUnsafeUDAFTest{
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName(s"${this.getClass.getCanonicalName}")
      .master("local[*]")
      .getOrCreate()

    val sales = Seq(
      (1, "Widget Co",        1000.00, 0.00,    "AZ", "2019-01-02"),
      (2, "Acme Widgets",     2000.00, 500.00,  "CA", "2019-02-01"),
      (3, "Widgetry",         1000.00, 200.00,  "CA", "2020-01-11"),
      (4, "Widgets R Us",     2000.00, 0.0,     "CA", "2020-02-19"),
      (5, "Ye Olde Widgete",  3000.00, 0.0,     "MA", "2020-02-28"))
    val salesDF = spark.createDataFrame(sales).toDF("id", "name", "sales", "discount", "state", "saleDate")
    salesDF.createTempView("sales")
    val yearOnYear = new TypeUnsafeUDAF
    spark.udf.register("YearOnYearBasisUDAF", yearOnYear)
    spark.sql("select YearOnYearBasisUDAF(sales, saleDate) as yearOnYear from sales").show()

    spark.stop()
  }
}