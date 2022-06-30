package cn.northpark.scalaSpark.lagouApp.graphx

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExample2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 生成图
    val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/graph.dat")

    graph.vertices.foreach(println)
    graph.edges.foreach(println)

    // 调用连通图算法
    graph.connectedComponents()
        .vertices
      .sortBy(_._2)
        .foreach(println)

    sc.stop()
  }
}
