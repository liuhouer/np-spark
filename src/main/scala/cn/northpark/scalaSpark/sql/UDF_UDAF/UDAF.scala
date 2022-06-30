package cn.northpark.scalaSpark.sql.UDF_UDAF

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *UserDefinedAggregateFunction 用户自定义聚合函数 抽象类
  */
class MyUDAF extends UserDefinedAggregateFunction  {
  //输入数据的类型
  def inputSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("uuuu", StringType, true)))
  }

  /**
    * 为每个分组的数据执行初始化值
    * 两个部分的初始化：
    *   1.在map端每个RDD分区内，在RDD每个分区内 按照group by 的字段分组，每个分组都有个初始化的值
    *   2.在reduce 端给每个group by 的分组做初始值
     */
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }
  // 每个组，有新的值进来的时候，进行分组对应的聚合值的计算
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0)+1
  }
  // 最后merger的时候，在各个节点上的聚合值，要进行merge，也就是合并
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  // 聚合操作时，所处理的数据的类型
  def bufferSchema: StructType = {
    DataTypes.createStructType(Array(DataTypes.createStructField("QQQQ", IntegerType, true)))
  }
  // 最后返回一个最终的聚合值要和dataType的类型一一对应
  def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
  // 最终函数返回值的类型
  def dataType: DataType = {
    DataTypes.IntegerType
  }
  //多次运行 相同的输入总是相同的输出，确保一致性
  def deterministic: Boolean = {
    true
  }
}

object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("UDAF").getOrCreate()
    val nameList: List[String] = List[String]("zhangsan", "lisi", "lisi", "lisi", "lisi", "wangwu", "zhangsan", "lisi", "zhangsan", "wangwu")
    import spark.implicits._
    val frame: DataFrame = nameList.toDF("name")
    frame.createOrReplaceTempView("students")

    //select  name ,count(*) from table group by name
    /**
      * 注册UDAF函数
      *
      */
    spark.udf.register("NAMECOUNT",new MyUDAF())

    spark.sql("select name,NAMECOUNT(name) as count from students group by name").show(100)
  }
}
