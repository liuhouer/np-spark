import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable

object SplitDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Demo1")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")

    val lst = List(
      UserBehavior("1", "1", 1, 1, List(Map("package" -> "A", "activetime" -> "a"),
        Map("package" -> "B", "activetime" -> "b"))),
      UserBehavior("2", "2", 2, 2, List(Map("package" -> "C", "activetime" -> "c"),
        Map("package" -> "D", "activetime" -> "d"),
        Map("package" -> "E", "activetime" -> "e")))
    )

    val map = Map("package" -> "D", "activetime" -> "d")
    println(map.mkString(";"))

    val rdd: RDD[UserBehavior] = spark.sparkContext.makeRDD(lst)
    rdd.map { elem =>
      val k: (String, String, Long, Long) = (elem.userId, elem.day, elem.begintime, elem.endtime)
      val v: immutable.Seq[(String, String)] = elem.data.map { elem =>
        (elem.getOrElse("package", ""), elem.getOrElse("activetime", ""))
      }
      (k, v)
    }.flatMapValues(x => x)
      .foreach(println)

    spark.close()
  }
}

case class UserBehavior(userId: String,
                        day: String,
                        begintime: Long,
                        endtime: Long,
                        data: List[Map[String, String]])