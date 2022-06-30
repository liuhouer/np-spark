package graphx

import org.apache.spark.{SparkConf, SparkContext}

class GraphXExample3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    sc.stop()
  }
}