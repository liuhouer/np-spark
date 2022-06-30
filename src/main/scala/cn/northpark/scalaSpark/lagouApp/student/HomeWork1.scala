package cn.northpark.scalaSpark.lagouApp.student

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object HomeWork1 {
  def main(args: Array[String]): Unit = {
    // 初始化,框架代码
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 养成习惯导入
    import spark.implicits._
    // 获取ip访问信息表，只获取第一列ip数据，数据类型是DataSet
    spark.read
      // 分隔符为 |
      .option("delimiter", "|")
      // 读取文件
      .csv("data/http.log")
      // 将第一列ip取出
      .map(row => row.getString(1))
      // 创建临时表
      .createOrReplaceTempView("t1")

    // 获取ip配置表，这里是数组，收回来，并且做成了广播变量
    val ipData: Array[(Long, Long, String)] = spark.read
      // 分隔符为 |
      .option("delimiter", "|")
      // 读取文件
      .csv("data/ip.dat")
      // 获取ip范围和对应地址
      .map(row => (row.getString(2).toLong, row.getString(3).toLong, row.getString(6)))
      // 收回来
      .collect()
    // 变成广播变量，顺便排个序
    val ipBC = spark.sparkContext.broadcast(ipData.sortBy(_._1))

    // IP地址是一个32位的二进制数，通常被分割为4个“8位二进制数”（也就是4个字节）。
    // IP地址通常用“点分十进制”表示成（a.b.c.d）的形式，其中，a,b,c,d都是0~255之间的十进制整数。
    // 例：点分十进IP地址（100.4.5.6），实际上是32位二进制数（01100100.00000100.00000101.00000110）
    // 32位二进制数就是4个字节，4个字节就是个int，当然也能变成long
    // 位运算是一种实现思路
    def ip2Long(ip: String): Long = {
      ip.split("\\.")
        .map(_.toLong)
        .fold(0L) { (buffer, elem) =>
          buffer << 8 | elem
        }
    }

    // 通过查询ip在哪个ip范围内，确定地址
    def getCityName(ip: Long): String = {
      // 获取广播变量
      val ips: Array[(Long, Long, String)] = ipBC.value
      // 对广播变量进行二分查找，因为广播变量之前已经做过排序
      var start = 0
      var end = ips.length - 1
      var middle = 0

      // 正常的二分查找，排序查找算法有很多啦
      while (start <= end) {
        middle = (start + end) / 2
        if ((ip >= ips(middle)._1) && (ip <= ips(middle)._2))
          return ips(middle)._3
        else if (ip < ips(middle)._1)
          end = middle - 1
        else
          start = middle + 1
      }
      "Unknown"
    }

    // 注册udf函数为下面sql使用
    spark.udf.register("ip2Long", ip2Long _)
    spark.udf.register("getCityName", getCityName _)
    // 普通的sql查询
    spark.sql(
      """
        |select getCityName(ip2Long(value)) as provice, count(1) as no
        |  from t1
        |group by getCityName(ip2Long(value))
        |""".stripMargin).show

    spark.close()
  }
}
