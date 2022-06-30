package cn.northpark.scalaSpark.lagouApp.student1.work04

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * 需求：使用鸢尾花数据集实现KNN算法
 * --本题实现步骤（使用鸢尾花数据集实现KNN算法）
 * --现有共144条数据，
 * 取文件IrisA，包括9条数据（每种类型的各3条），作为测试集
 * 取文件IrisB，剩余135条数据，作为训练集
 *
 * --算法：
 * 1）读文件IrisA，IrisB，形成数据集X，Y
 * 2）求数据集X中每个点，到数据集Y中每个点的距离D
 * 3）找到数据集D中的最小的K个数
 * 4）求K个点的分类情况
 */
object KNN {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName.init)
      .getOrCreate()

    val X = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("src/data/IrisA.csv")

    val Y = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .csv("src/data/IrisB.csv")

    val D = X.rdd.cartesian(Y.rdd).map { case (x, y) =>
      val id = x.getInt(0)
      val x1 = x.getDouble(1)
      val x2 = x.getDouble(2)
      val x3 = x.getDouble(3)
      val x4 = x.getDouble(4)
      val kind = x.getString(5)

      val y1 = y.getDouble(1)
      val y2 = y.getDouble(2)
      val y3 = y.getDouble(3)
      val y4 = y.getDouble(4)
      // 计算距离
      val distance = Math.sqrt(
        Math.pow(y1 - x1, 2) +
          Math.pow(y2 - x2, 2) +
          Math.pow(y3 - x3, 2) +
          Math.pow(y4 - x4, 2)
      )
      (id, (distance, kind))
    }

    val K = 9

    D.groupByKey().mapValues { v =>
      val tuples = v.toList
        //根据距离排序取 K个
        .sortBy(_._1).take(K)
        // 根据品种分组
        .groupBy(_._2)
        .toList

      // 获取品种
      val kind: String = tuples.map(_._1).mkString
      // 获取距离
      val distance: Seq[Double] = tuples.head._2.map(_._1)
      // 求距离的平均值
      val rate = distance.sum / distance.size
      (kind, rate)
    }
      .sortByKey()
      .foreach(x => println({
        f"id: ${x._1}, 品种：${x._2._1}, 相似率：${x._2._2}%.2f"
      }))

    spark.stop()
  }
}
