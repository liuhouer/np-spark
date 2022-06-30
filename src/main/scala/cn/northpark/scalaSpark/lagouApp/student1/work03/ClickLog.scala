package cn.northpark.scalaSpark.lagouApp.student1.work03

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 用Spark-Core实现统计每个adid的曝光数与点击数，将结果输出到hdfs文件；
 * 保存格式: adid,点击数,曝光数
 */
object ClickLog {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName.init))

        //读取点击数据，返回string  map（点击数，曝光数）
        val clickRDD = sc.textFile("src/data/click.log").map { line =>
            val fields = line.split("\\s+")
            (fields(3).split("=")(5), (1, 0))
        }

        //读取曝光数据，返回string  map（曝光数，点击数）
        val impRDD = sc.textFile("src/data/imp.log").map { line =>
            val fields = line.split("\\s+")
            (fields(3).split("=")(5), (0, 1))
        }

        clickRDD.union(impRDD)
            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
            .map { x => x._1 + "," + x._2._1 + "," + x._2._2 }
            .coalesce(1)
            .saveAsTextFile("hdfs://Linux100:9000/data/cli_log")

        sc.stop()
    }
}
