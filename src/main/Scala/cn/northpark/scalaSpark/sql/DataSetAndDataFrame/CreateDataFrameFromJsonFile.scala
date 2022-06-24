package cn.northpark.scalaSpark.sql.DataSetAndDataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 读取json格式的文件加载DataFrame
  * 注意：
  *   1.读取json格式的两种方式
  *   2.df.show默认显示前20行，使用df.show(行数)显示多行
  *   3.dataFrame加载过来会按照列的ascii码排序
  *   4.df.printSchema显示列的Schema信息
  *   5.创建临时表的两种方式和区别：createOrReplaceTempView  | createGlobalTempView
  */
object CreateDataFrameFromJsonFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataSetFromJsonFile")
      .master("local")
      .getOrCreate()

    /**
      * 需要导入隐式转换，下面的$ toDF都会用到隐式转换
      */

//    val df: DataFrame = spark.read.json("./data/json")
//    val df: DataFrame= spark.read.format("json").load("./data/json")
    //显示DataFrame，默认显示前20行
//    df.show()
//    //指定显示的行数
//    df.show(100)
//    //打印schema信息
//    df.printSchema()

    /**
      * 获取值
      */
//    val rows1: Array[Row] = df.take(100)
//    val row2: Row = df.first()
//    val rows3: Array[Row] = df.head(4)
//    println(rows1.toBuffer)
//    println(row2)
//    println(rows3.toBuffer)

    /**
      * 使用DataFrame原生api
      */
      import  spark.implicits._
//    val frame: DataFrame = df.select(df.col("name"))
//    val frame = df.select("name")
//    val frame = df.select("name","age")
//    val frame = df.filter($"age">18)//查找年龄大于18的人
//    val frame = df.filter(df.col("name").equalTo("zhangsan"))
//    val frame = df.filter("name='zhangsan4' or name = 'zhangsan5'")//可以写表达式
//    val frame = df.sort($"age".asc,$"name".desc)//按照age升序，按照name降序排列
//    val frame = df.sort(df.col("age").asc,df.col("name").desc)//按照age升序，按照name降序排列
//    val frame = df.select(df.col("name").as("studentName"),df.col("age").alias("studentAge"))//给定别名
//    val frame = df.select($"name".alias("studentName"),$"age")//给定别名
//    val frame = df.select($"name",($"age"+1).as("addAge"))//age+1之后，直接列名 成为了 age+1
//    val frame = df.groupBy("age").count()//按照age 分组
//    frame.show(100)


    /**
      * 使用sql 操作
      *
      *   createOrReplaceTempView:创建临时的视图
      *   createGlobalTempView:创建全局的视图，访问全局的视图使用  global_temp.表名,全局的表可以跨spark session访问
      *
      */
//    df.createOrReplaceTempView("students")
//    df.createGlobalTempView("students")
//    val frame: DataFrame = spark.sql("select  * from students")
//    frame.show()
//    import spark._
//    sql("select * from students where age > 18 and name = 'zhangsan5'").show(100)
//    sql("select  * from global_temp.students where name like 'wang%'").show(100)
//
//    //创建一个新的session
//    spark.newSession().sql("SELECT * FROM global_temp.students").show()
//    spark.newSession().sql("SELECT * FROM students").show()

    /**
      * 将DataFrame转换成RDD
      */
//    val rdd: RDD[Row] = df.rdd
//    rdd.foreach(row=>{
//      //打印row
//      println(row)
//      /**
//        * row中获取值,两种方式 1. getAs("字段名") 2.getAs(下标)
//        */
//      val name = row.getAs[String]("name")
//      val age = row.getAs[Long]("age")
//
////      val name = row.getAs[String](1)
////      val age = row.getAs[Long](0)
////      println("name = "+name+",age = "+age)
//    })

    spark.stop()
  }
}
