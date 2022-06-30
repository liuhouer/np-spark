import org.joda.time.DateTime

import scala.math.random

object test2 {
  def main(args: Array[String]): Unit = {
    (1 to 100).foreach{
      idx => val (x, y) = (random, random)
        println(idx, x, y)
    }


    // 1562085684177 => Hour
    val str = "1562085684177"
    // 在spark core程序中一定不要使用java8以前的时间日期类型。线程不安全
    // 使用第三方的时间日期类型包，一定要确认其是线程安全的！！！
    val dt = new DateTime(str.toLong)
    val hour: Int = dt.getHourOfDay
    println(hour)

    println("*************************************************")
    val arr = (1 to 5).toArray
    arr.combinations(2).foreach(x=>println(x.toBuffer))


    val s1 = (1 to 5).toSet
    val s2 = (3 to 8).toSet
    // 交。intersect
    println(s1 & s2)
    // 并。union
    println(s1 | s2)
    // 差。diff
    println(s1 &~ s2)


    val words = "ob,j?e'cts)."
    println(words.replace(")", "").replace(".", ""))
    println(words.replaceAll("[\\)\\.,:;'!\\?]", ""))

    val arr1 = "in on to a an the are were si was".split("\\s+")
    println(arr1.contains("from"))

  }
}