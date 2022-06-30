package graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExample1 {
  def main(args: Array[String]): Unit = {
    // 初始化
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 初始化数据
    // 定义顶点 (Long, info)
    val vertexArray: Array[(VertexId, (String, Int))] = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    // 定义边 (Long, Long, attr)
    val edgeArray: Array[Edge[Int]] = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    // 构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(edgeArray)

    // 构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

//    graph.pageRank()

    // 属性操作示例
    // 找出图中年龄大于30的顶点
    graph.vertices
      .filter { case (_, (_, age)) => age > 30 }
      .foreach(println)

    // 找出图中属性大于5的边
    graph.edges
        .filter{edge => edge.attr>5}
        .foreach(println)

    // 列出边属性>5的tripltes
    graph.triplets
        .filter(t => t.attr > 5)
        .foreach(println)

    // degrees操作
    // 找出图中最大的出度、入度、度数
    println("*********** 出度 ***********")
    graph.outDegrees.foreach(println)
    val outDegress: (VertexId, Int) = graph.outDegrees
        .reduce{(x, y) => if (x._2 > y._2) x else y}
    println(s"outDegress = $outDegress")

    println("*********** 入度 ***********")
    graph.inDegrees.foreach(println)
    val inDegress: (VertexId, Int) = graph.inDegrees
      .reduce{(x, y) => if (x._2 > y._2) x else y}
    println(s"inDegress = $inDegress")

    println("*********** 度数 ***********")
    graph.degrees.foreach(println)
    val degress: (VertexId, Int) = graph.degrees
        .reduce{(x, y) => if (x._2 > y._2) x else y}
    println(s"degress = $degress")

    // 转换操作
    // 顶点的转换操作。所有人的年龄加 10 岁
    graph.mapVertices{case (id, (name, age)) => (id, (name, age+10))}
        .vertices
        .foreach(println)

    // 边的转换操作。边的属性*2
    graph.mapEdges(e => e.attr*2)
        .edges
        .foreach(println)

    // 结构操作
    // 顶点年龄 > 30 的子图
     val subGraph: Graph[(String, Int), Int] = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)
    println("************** 子图 ***************")
    subGraph.edges.foreach(println)
    subGraph.vertices.foreach(println)

    // 连接操作
    println("*********************** 连接操作 ***********************")
    // 创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
    val initialUserGraph: Graph[User, Int] = graph.mapVertices { case (_, (name, age)) => User(name, age, 0, 0) }

    // initialUserGraph与inDegrees、outDegrees 进行 join，修改 inDeg、outDeg
    val userGraph: Graph[User, Int] = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
      case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
    }.outerJoinVertices(initialUserGraph.outDegrees) {
      case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
    }

    userGraph.vertices.foreach(println)

    // 找到 出度=入度 的人员
    userGraph.vertices.filter { case (id, u) => u.inDeg == u.outDeg }
        .foreach(println)

    // 聚合操作
    // 找出5到各顶点的最短距离
    val sourceId: VertexId = 5L // 定义源点
    val initialGraph: Graph[Double, Int] = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp: Graph[Double, Int] = initialGraph.pregel(Double.PositiveInfinity)(
      // 两个消息来的时候，取它们当中路径的最小值
      (id, dist, newDist) => math.min(dist, newDist),

      // Send Message函数
      // 比较 triplet.srcAttr + triplet.attr和 triplet.dstAttr。如果小于，则发送消息到目的顶点
      triplet => { // 计算权重
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },

      // mergeMsg
      (a, b) => math.min(a, b) // 最短距离
    )

    println("找出5到各顶点的最短距离")
    println(sssp.vertices.collect.mkString("\n"))

    sc.stop
  }
}

case class User(name: String, age: Int, inDeg: Int, outDeg: Int)



//val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
//triplet => {
//if (triplet.srcAttr._2 > triplet.dstAttr._2) {
//triplet.sendToDst(1, triplet.srcAttr._2)
//}
//},
//// Add counter and age
//(a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
//)
//
//val avgAgeOfOlderFollowers: VertexRDD[Double] =
//olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
//println("***************** 聚合操作 **********************")
//avgAgeOfOlderFollowers.collect.foreach(println(_))
//println("***************** 聚合操作 **********************")

// 这个没看明白
// http://www.voidcn.com/article/p-padrwkwf-qg.html