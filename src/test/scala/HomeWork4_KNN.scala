import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{pow, sqrt}

object HomeWork4_KNN {
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
    val sampleRDD: RDD[LabelPoint] = lines.filter(_.label != "")
    val testData: Array[Array[Double]] = lines.filter(_.label == "").collect().map(_.point)

    def getDistance(x: Array[Double], y: Array[Double]): Double = {
      sqrt(x.zip(y).map(z => pow(z._1 - z._2, 2)).sum)
    }

    // 4、求最近的K个点；对这K个点的label做wordcount，得到最终结果
    testData.foreach(elem => {
      val dists: RDD[(Double, String)] = sampleRDD.map(labelpoint => (getDistance(elem, labelpoint.point), labelpoint.label))
      val minDists: Array[(Double, String)] = dists.sortBy(_._1).take(K)
      val labels = minDists.map(_._2)
      print(s"${elem.toBuffer} : ")
      labels.groupBy(x=>x).mapValues(_.length).foreach(print)
      println()
    })

    sc.stop()
  }
}
