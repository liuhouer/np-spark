package cn.northpark.sql.DataSetAndDataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 由json格式的RDD转换成DataFrame,在旧的版本中 spark.read.json(jsonRDD<String>) ,这中方式2.3+被丢弃
  * 目前来说 2.3+版本 中直接就是将json格式的DataSet转换成DataFrame
  *
  * 注意：
  *    1.加载成的DataFrame会自动按照列的Ascii码排序
  *    2.自己写sql 组成的DataFrame的列不会按照列的Ascii码排序
  */
object CreateDataFrameFromJsonDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("createDFFromJsonRDD").master("local").getOrCreate()
    val jsonList = List[String](
      "{\"name\":\"zhangsan\",\"age\":20}",
      "{\"name\":\"lisi\",\"age\":21}",
      "{\"name\":\"wangwu\",\"age\":22}"
    )
    val jsonList2 = List[String](
      "{\"name\":\"zhangsan\",\"score\":100}",
      "{\"name\":\"lisi\",\"score\":200}",
      "{\"name\":\"wangwu\",\"score\":300}"
    )

    /**
      * 1.6版本方式
      */
//    val jsonRDD: RDD[String] = spark.sparkContext.parallelize(jsonList)
//    val df1: DataFrame = spark.read.json(jsonRDD)
//    df1.show(100)

    /**
      * 2.3+版本
      */

    import spark.implicits._

    val jsonDs: Dataset[String] = jsonList.toDS()
    val scoreDs: Dataset[String] = jsonList2.toDS()
    val df2: DataFrame = spark.read.json(jsonDs)
    val df3 :DataFrame = spark.read.json(scoreDs)
    df2.show()

    df2.createOrReplaceTempView("person")
    df3.createOrReplaceTempView("score")
    val frame: DataFrame = spark.sql("select * from person")
    frame.show()

    spark.sql("select t1.name ,t1.age,t2.score from person  t1, score  t2 where t1.name = t2.name").show()

  }
}
