package cn.lagou.tmp

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object TopN{
  def main(args: Array[String]): Unit = {
    // 创建SparkContext
    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName.init).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val N = 8

    // 生成数据
    val random = scala.util.Random
    val scores: immutable.IndexedSeq[String] = (1 to 50).flatMap { idx =>
      (1 to 2000).map { id =>
        f"group$idx%3d,${random.nextInt(10000)}"
      }
    }

    val scoreRDD: RDD[(String, Int)] = sc.makeRDD(scores).map { line =>
      val fields = line.split(",")
      (fields(0), fields(1).toInt)
    }
    scoreRDD.cache()

    // topN方法一
    scoreRDD.groupByKey()
      .mapValues(buf => buf.toList.sorted.reverse.take(N))
      .sortByKey()
      .collect()
      .foreach(println)

    // topN优化
    scoreRDD.aggregateByKey(List[Int]())(
      (lst, score) => (lst :+ score).sorted.takeRight(N).reverse,
      (lst1, lst2) => (lst1 ::: lst2).sorted.takeRight(N).reverse
    ).sortByKey()
    .collect
        .foreach(println)

    // 关闭SparkContext
    sc.stop()
  }
}
