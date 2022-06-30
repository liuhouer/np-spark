import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.math.{pow, sqrt}

object HomeWork4_SuperKNN {
  case class LabelPoint(label: String, point: Array[Double])

  def main(args: Array[String]): Unit = {
    // 1、初始化
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getCanonicalName}")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    val K = 15
    sc.setLogLevel("WARN")

    // 2、读数据，封装数据
    val lines = sc.textFile("data\\Iris.csv")
      .map(line => {
        val fields = line.split(",")
        if (fields.length==5)
          LabelPoint("", fields.tail.map(_.toDouble))
        else
          LabelPoint(fields.last, fields.init.tail.map(_.toDouble))
      })

    // 3、将数据分为样本数据、测试数据
    val sampleRDD = lines.filter(_.label != "")
    val testData = lines.filter(_.label == "").collect().map(_.point)
    val bc_testData = sc.broadcast(testData)

    def getDistance(x: Array[Double], y: Array[Double]): Double = {
      sqrt(x.zip(y).map(z => pow(z._1 - z._2, 2)).sum)
    }

    // 4、计算距离
    // 减少对数据集的扫描（mapPartitions、广播变量），以应对大数据集（优化）
    val distanceRDD: RDD[(String, (Double, String))] = sampleRDD.mapPartitions(iter => {
      val bcpoints = bc_testData.value
      // 这里要用flatMap
      iter.flatMap { case LabelPoint(label, point1) =>
        bcpoints.map(point2 => (point2.mkString(","), (getDistance(point2, point1), label)))
      }
    })

    // 5、求距离最小的K个点
    // 转换为了一个典型的 TopK 问题 (求分区内的topK，再求总的topK)
    //    val topKRDD: RDD[(String, Map[String, Int])] = distanceRDD.aggregateByKey(immutable.TreeSet[(Double, String)]())(
    //      (splitSet: TreeSet[(Double, String)], elem: (Double, String)) => {
    //        val newSet = splitSet + elem
    //        newSet.take(K)
    //      },
    //      (splitSet1: TreeSet[(Double, String)], splitSet2) =>
    //        (splitSet1 ++ splitSet2).take(K)
    //    ).map { case (point, set) =>
    //      (point, set.toArray.map(_._2).groupBy(x => x).map(x => (x._1, x._2.length)))
    //    }
    val topKRDD = distanceRDD.combineByKey(
      (elem) => TreeSet(elem),
      (splitSet: immutable.TreeSet[(Double, String)], elem) => {
        val newSet = splitSet + elem
        newSet.take(K)
      },
      (set1: immutable.TreeSet[(Double, String)], set2: immutable.TreeSet[(Double, String)]) => (set1 ++ set2).take(K)
    ).map { case (point, set) =>
      (point, set.toArray.map(_._2).groupBy(x => x).map(x => (x._1, x._2.length)))
    }

    // 6、打印结果
    topKRDD.collect().foreach(println)

    sc.stop()
  }
}