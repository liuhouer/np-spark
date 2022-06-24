package cn.northpark.sql.UDF_UDAF

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * UDF 用户自定义函数
  */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("UDF").getOrCreate()
    val nameList: List[String] = List[String]("zhangsan", "lisi", "wangwu", "zhaoliu", "tianqi")
    import spark.implicits._
    val nameDF: DataFrame = nameList.toDF("name")
    nameDF.createOrReplaceTempView("students")
//    nameDF.show()

    spark.udf.register("STRLEN",(name:String)=>{
      name.length
    })
    spark.sql("select name ,STRLEN(name) as length from students sort by length desc").show(100)
  }
}
