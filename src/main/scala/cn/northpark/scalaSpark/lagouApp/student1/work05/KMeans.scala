package cn.northpark.scalaSpark.lagouApp.student1.work05

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 需求：使用鸢尾花数据集实现KMeans算法
 * --K-Means聚类算法
 * --本题实现步骤（使用鸢尾花数据集实现KMeans算法）
 * --算法：
 * 1）读文件
 * 2）随机找K个点，作为初始的中心点
 * 3）遍历数据集，计算每一个点与3个中心点的距离，距离哪个中心点最近就属于哪个中心点
 * 4）根据新的分类计算新的中心点
 * 5）使用新的中心点开始下一次循环 -> 步骤3
 *
 * -- 退出循环的条件
 * 1）指定循环次数
 * 2）所有的中心点几乎不再移动（即中心点移动的最大距离小于我们给定的一个常数，如：0.01）
 * 修正：所有中心点移动距离的总和小于一个给定的常数
 */
object KMeans {

    case class LabelPoint(label: String, point: Array[Double])

    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.init))

        // 读文件
        val lines = sc.textFile("src/data/Iris.csv")
        val data = lines
            .zipWithIndex().filter(_._2 >= 1).keys // 过滤表头
            .map { line =>
                val fields = line.split(",")
                LabelPoint(fields.last, fields.init.tail.map(_.toDouble))
            }

        // 获取K个随机的中心点
        val K = 3
        val centerPoint = data.takeSample(withReplacement = false, K).map(_.point)
        val minDistance = 0.01
        var tempDistance = 1.0

        while (tempDistance > minDistance) {
            val newCenterPoint = data.map(p => (getIndex(p.point, centerPoint), (p.point, 1.0))) // 得到每个点的分类 [分类编号, (特征, 1.0)]；
                .reduceByKey((x, y) => (getNewCenter(x._1, y._1), x._2 + y._2)) // 计算新的中心点
                .map { case (index, (point, count)) =>
                    (index, point.map(_ / count))
                }.collectAsMap()

            // 计算中心点移动的距离
            val distance = for (i <- 0 until K) yield getDistance(centerPoint(i), newCenterPoint(i))
            tempDistance = distance.sum

            // 重新定义中心点
            for ((k, v) <- newCenterPoint) centerPoint(k) = v
        }
        println("中心点位置：")
        centerPoint.foreach(x => println(x.toList))

        sc.stop()
    }

    /**
     * 获取距离
     */
    def getDistance(x: Array[Double], y: Array[Double]): Double = {
        math.sqrt(x.zip(y).map(e => math.pow(e._1 - e._2, 2)).sum)
    }

    /**
     * 获取索引
     */
    def getIndex(p: Array[Double], center: Array[Array[Double]]): Int = {
        val distance = center.map(point => getDistance(point, p))
        distance.indexOf(distance.min)
    }

    /**
     * 获取新的中心
     */
    def getNewCenter(x: Array[Double], y: Array[Double]): Array[Double] = {
        x.zip(y).map(e => e._1 + e._2)
    }
}
