package cn.northpark.scalaSpark.lagouApp.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXExample3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getCanonicalName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("warn")

    // 原始数据集
    val lst: List[(List[Long], List[(String, Double)])] = List(
      (List(11L, 21L, 31L), List("kw$北京" -> 1.0, "kw$上海" -> 1.0, "area$中关村" -> 1.0)),
      (List(21L, 32L, 41L), List("kw$上海" -> 1.0, "kw$天津" -> 1.0, "area$回龙观" -> 1.0)),
      (List(41L), List("kw$天津" -> 1.0, "area$中关村" -> 1.0)),
      (List(12L, 22L, 33L), List("kw$大数据" -> 1.0, "kw$spark" -> 1.0, "area$西二旗" -> 1.0)),
      (List(22L, 34L, 44L), List("kw$spark" -> 1.0, "area$五道口" -> 1.0)),
      (List(33L, 53L), List("kw$hive" -> 1.0, "kw$spark" -> 1.0, "area$西二旗" -> 1.0))
    )
    val rawRDD: RDD[(List[Long], List[(String, Double)])] = sc.makeRDD(lst)

    // 创建边。RDD[Edge(Long, Long, T2)]
    // List(11L, 21L, 31L), A1 => 11 -> 112131, 21 -> 112131, 31 -> 112131
    val dotRDD: RDD[(Long, Long)] = rawRDD.flatMap { case (ids, _) =>
      ids.map(id => (id, ids.mkString.toLong))
    }
    val edgesRDD: RDD[Edge[Int]] = dotRDD.map { case (id, ids) => Edge(id, ids, 0) }

    // 创建顶点。RDD[(Long, T1)]
    val vertexesRDD: RDD[(Long, String)] = dotRDD.map { case (id, ids) => (id, "") }

    // 生成图
    val graph: Graph[String, Int] = Graph(vertexesRDD, edgesRDD)

    // 调用强连通体算法。识别6条数据，代表2个不同的用户
    val connectedRDD: VertexRDD[VertexId] = graph.connectedComponents()
      .vertices
//    connectedRDD.foreach(println)

    // 定义中心的数据
    val centerVertexRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] =
      rawRDD.map { case (ids, info) => (ids.mkString.toLong, (ids, info)) }
//    centerVertexRDD.foreach(println)

    // join操作，拿到分组的标记
    val dataRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] = connectedRDD.join(centerVertexRDD)
      .map { case (_, (v1, v2)) => (v1, v2) }

    // 数据聚合、合并
    val resultRDD: RDD[(VertexId, (List[VertexId], List[(String, Double)]))] = dataRDD.reduceByKey { case ((bufIds, bufInfo), (ids, info)) =>
      // 数据聚合
      val newIds: List[VertexId] = bufIds ++ ids
      val newInfo: List[(String, Double)] = bufInfo ++ info

      // 对用户id做去重；对标签做合并
      (newIds.distinct, newInfo.groupBy(_._1).mapValues(lst => lst.map(_._2).sum).toList)
    }

    resultRDD.foreach(println)

    sc.stop()
  }
}