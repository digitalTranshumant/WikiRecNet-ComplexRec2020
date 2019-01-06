package edu.ucu.graph

import scala.util.Random
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object RandomWalkGenerator {

  val N_WALKS = 50

  def takeSample(a:Array[VertexId], n: Int): Array[VertexId] = {
    val rnd = new Random(42)
    if (a.length > 0) Array.fill(n)(a(rnd.nextInt(a.length))) else a
  }

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("yarn").setAppName("Random Walk")
      //.set("spark.driver.memory", "8g")
      //.set("spark.memory.storageFraction", "0.7")
      .set("spark.rdd.compress", "true")

    val sc = SparkContext.getOrCreate(config)

    val graph = GraphLoader.edgeListFile(sc, args(0),
      edgeStorageLevel = StorageLevel.MEMORY_ONLY_SER,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY_SER
    ).mapVertices((id, attr) => Array(id))


    val initialMessage = Array.empty[VertexId]
    val numWalks = 4

    def vertexProgram(id: VertexId, attr: Array[VertexId], sum: Array[VertexId]) = takeSample(sum ++ attr, N_WALKS)
    def sendMessage(edge: EdgeTriplet[Array[VertexId], Int]): Iterator[(VertexId, Array[VertexId])] =
      Iterator(
        (edge.dstId, edge.srcAttr),
        (edge.srcId, edge.dstAttr))

    def messageCombiner(a: Array[VertexId], b: Array[VertexId]) = a ++ b

    val gWithNeighbors = Pregel(graph, initialMessage, numWalks)(vertexProgram, sendMessage, messageCombiner)
    gWithNeighbors.vertices.take(20).map(x => (x._1, x._2.toList)).foreach(println)

    //println(graph.vertices.count(), graph.edges.count())

    sc.stop()
  }
}
