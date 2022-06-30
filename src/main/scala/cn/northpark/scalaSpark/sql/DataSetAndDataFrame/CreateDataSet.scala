package cn.northpark.scalaSpark.sql.DataSetAndDataFrame

import org.apache.spark.sql.{Dataset, SparkSession}

case class Student(name:String,age:Long)
case class Person(id:Int,name:String,age:Int,score:Double)

/**
  * 创建DataSet
  */
object CreateStructDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createStruceDataSet").getOrCreate()
    import spark.implicits._
    /**
      * 1. 由集合创建DataSet
      *
      */
    //直接映射成Person类型的DataSet
    val list = List[Person](
      Person(1,"zhangsan",18,100),
      Person(2,"lisi",19,200),
      Person(3,"wangwu",20,300)
    )
    val personDs: Dataset[Person] = list.toDS()
    personDs.show(100)

    //直接由集合得到
    val value: Dataset[Int] = List[Int](1, 2, 3, 4, 5).toDS()
    value.show()

    /**
      * 2.由json文件和类直接映射成DataSet
      */

    val lines: Dataset[Student] = spark.read.json("./data/json").as[Student]
    lines.show()

    /**
      * 3.读取外部文件直接加载DataSet
      */
    val dataSet: Dataset[String] = spark.read.textFile("./data/people.txt")
    val result: Dataset[Person] = dataSet.map(line => {
      val arr: Array[String] = line.split(",")
      Person(arr(0).toInt, arr(1).toString, arr(2).toInt, arr(3).toDouble)
    })
    result.show()

  }
}
