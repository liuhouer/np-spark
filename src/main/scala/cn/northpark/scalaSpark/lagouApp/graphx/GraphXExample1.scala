package cn.northpark.scalaSpark.lagouApp.graphx

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExample1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 定义顶点
    val vertexArray: Array[(VertexId, (String, Int))] = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val vertexRDD: RDD[(VertexId, (String, Int))] = sc.makeRDD(vertexArray)

    // 定义边
    val edgeArray: Array[Edge[Int]] = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 6),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(edgeArray)

    // 图的定义
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    // 属性操作(找出图中年龄 > 30 的顶点；属性 > 5 的边； 属性 > 5 的 triplets)
//    graph.vertices
//        .filter{case (_, (_, age)) => age > 30}
//        .foreach(println)
//
//    graph.edges
//        .filter(edge => edge.attr > 5)
//      .foreach(println)
//
//    graph.triplets
//      .filter { t => t.attr > 5 }
//      .foreach(println)

    // 属性操作。degress操作，找出图中最大的出度、入度、度数
//    val inDegress: (VertexId, Int) = graph.inDegrees
//      .reduce((x, y) => if (x._2 > y._2) x else y)
//    println(s"inDegress = $inDegress")
//
//    val outDegress: (VertexId, Int) = graph.outDegrees
//      .reduce((x, y) => if (x._2 > y._2) x else y)
//    println(s"outDegress = $outDegress")
//
//    val degress: (VertexId, Int) = graph.degrees
//      .reduce((x, y) => if (x._2 > y._2) x else y)
//    println(s"degress = $degress")

    // 转换操作。顶点转换，所有人年龄加 100
//    graph.mapVertices{case (id, (name, age)) => (id, (name, age+100))}
//        .vertices
//        .foreach(println)

    // 边的转换，边的属性*2
//    graph.mapEdges(e => e.attr * 2)
//        .edges
//        .foreach(println)

    // 结构操作。顶点年龄 > 30 的子图
//    val subGraph: Graph[(String, Int), Int] = graph.subgraph(vpred = (id, vd) => vd._2 > 30)
//    subGraph.edges.foreach(println)
//    subGraph.vertices.foreach(println)

    // 找出出度=入度的人员。连接操作
    // 思路：图 + 顶点的出度 + 顶点的入度 => 连接操作
//    val initailUserGraph: Graph[User, Int] = graph.mapVertices { case (id, (name, age)) => User(name, age, 0, 0) }
//
//    val userGraph: Graph[User, Int] = initailUserGraph.outerJoinVertices(initailUserGraph.inDegrees) {
//      case (id, u, inDeg) => User(u.name, u.age, inDeg.getOrElse(0), u.outDegress)
//    }.outerJoinVertices(initailUserGraph.outDegrees) {
//      case (id, u, outDeg) => User(u.name, u.age, u.inDegress, outDeg.getOrElse(0))
//    }
//    userGraph.vertices.filter{case (_, user) => user.inDegress==user.outDegress}
//        .foreach(println)


    // 顶点5到其他各顶点的最短距离。聚合操作(Pregel API)
    val sourceId: VertexId = 5L
    val initailGraph: Graph[Double, Int] = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val disGraph: Graph[Double, Int] = initailGraph.pregel(Double.PositiveInfinity)(
      // 两个消息来的时候，取其中的最小路径
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message 函数
      triplet => {
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else
          Iterator.empty
      },

      // mergeMsg
      (dista, distb) => math.min(dista, distb)
    )

    disGraph.vertices.foreach(println)

    sc.stop()
  }
}

case class User(name: String, age: Int, inDegress: Int, outDegress: Int)