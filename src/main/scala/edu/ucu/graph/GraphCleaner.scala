package edu.ucu.graph

import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession

case class Node(inDegree: Int, outDegree: Int, outRate: Int)


object GraphCleaner {

  case class Config(
                     inputEdgesPath: String = "",
                     outputEdgesPath: String = "",
                     maxOutRate: Int = 20,
                     mode: String = "filter"
                   )

  private val confBuild = new scopt.OptionParser[Config]("GraphCleaner") {
    opt[String]("input").required().action((x, c) => c.copy(inputEdgesPath = x))
    opt[String]("output").action((x, c) => c.copy(outputEdgesPath = x))
    opt[Int]("max-rate").action((x, c) => c.copy(maxOutRate = x))

    cmd("analyze").action((_, c) => c.copy(mode = "analyze"))
  }

  def preprocess[VD, ED](graph: Graph[VD, ED]): Graph[Node, ED] = {
    val outDegrees: VertexRDD[Int] = graph.ops.outDegrees
    val inDegrees: VertexRDD[Int] = graph.ops.inDegrees
    graph.outerJoinVertices(outDegrees) { (vid, data, deg) => deg.getOrElse(0) }
      .outerJoinVertices(inDegrees) {
        (vid, outDegree, deg) => {
          val inDegree = deg.getOrElse(0)
          Node(deg.getOrElse(0), outDegree, if (inDegree > 0) outDegree / inDegree else 0)
        }
      }
  }

  def main(args: Array[String]): Unit = {
    val config = confBuild.parse(args, Config()).get

    val spark = SparkSession.builder().master("yarn")
      .appName("counter").getOrCreate()

    val graph = GraphLoader.edgeListFile(spark.sparkContext, config.inputEdgesPath)

    if (config.mode == "filter") {
      val filteredGraph = graph.filter(
        preprocess,
        vpred = (_: VertexId, deg: Node) => {
          deg match {
            case Node(in, out, outRate) => in > 0 && out > 0 && outRate <= config.maxOutRate
          }
        }
      )

      println(filteredGraph.vertices.count(), filteredGraph.edges.count())

      if (config.outputEdgesPath.nonEmpty) {
        filteredGraph.edges.map { e => s"${e.srcId}\t${e.dstId}" }.saveAsTextFile(config.outputEdgesPath)
      }
    } else {
      val outRates = preprocess(graph).vertices.map{ case (_, n) => n.outRate }.countByValue()
      outRates.toList.sortBy(-_._1).take(100).foreach(println)
    }

  }

}
