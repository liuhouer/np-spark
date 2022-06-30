package cn.northpark.scalaSpark.lagouApp.advanced

import java.io.{File, PrintWriter}

// 写数据文件，用于后面的测试
object WriteFile {
  def main(args: Array[String]): Unit = {
    val writer = new PrintWriter(new File("data/log1.txt" ))

    val str1 =
      """
        |hello spark hello java hello spark hello java hello scala hello hbase hello hive hello kafka
        |allows scala example are two hive configure can when with eg some master only as we Note
        |follows arbitrary application parallelism directly separately initialize set kafka well
        |SparkContext passed configured
        |""".stripMargin
    val str: String = (1 to 300000).map(idx => str1).mkString(" ")

    writer.write(str)
    writer.close()
  }
}