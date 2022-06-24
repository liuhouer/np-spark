package cn.northpark.sql

import java.util.Properties

import org.apache.spark.sql.{ Dataset, Row, SparkSession}

object CreateDataFrameFromMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val result: Dataset[Row] = spark.read.jdbc("jdbc:mysql://192.168.179.4:3306/test", "(" +
      "SELECT  t1.device_id,t1.app_type,t1.app_list  " +
      "FROM second t1 , (select distinct device_id,app_type from  first ) t2 " +
      "WHERE t1.device_id = t2.device_id and t1.app_type !=t2.app_type) T", properties)

    result.show()
  }
}
