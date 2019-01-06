package edu.ucu.kcore

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.math._
import scala.reflect.ClassTag

object CommunityUtils extends Logging {

  val RED = "\033[1;30m"
  val ENDC = "\033[0m"

  /**
    * splitCommunity
    *
    * Find and split communities in graph
    *
    * @param Graph[String,String] $graph - Graph element
    * @param RDD[(VertexId, (String))] $users - Vertices
    * @param Boolean $displayResult - if true, display println
    * @return ArrayBuffer[Graph[String,String]] - Contains one graph per community
    *
    */
  def splitCommunity(graph: Graph[String, String], NBKCORE: Int, displayResult: Boolean): Graph[Int, String] = {

    println(color("\nCall SplitCommunity", RED))

    getKCoreGraph(graph, NBKCORE, displayResult).cache()
  }

  /**
    * Compute the k-core decomposition of the graph for all k <= kmax. This
    * uses the iterative pruning algorithm discussed by Alvarez-Hamelin et al.
    * in K-Core Decomposition: a Tool For the Visualization of Large Scale Networks
    * (see <a href="http://arxiv.org/abs/cs/0504107">http://arxiv.org/abs/cs/0504107</a>).
    *
    * @tparam VD the vertex attribute type (discarded in the computation)
    * @tparam ED the edge attribute type (preserved in the computation)
    *
    * @param graph the graph for which to compute the connected components
    * @param kmax the maximum value of k to decompose the graph
    *
    * @return a graph where the vertex attribute is the minimum of
    *         kmax or the highest value k for which that vertex was a member of
    *         the k-core.
    *
    * @note This method has the advantage of returning not just a single kcore of the
    *       graph but will yield all the cores for k > kmin.
    */
  def getKCoreGraph[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED],
                                                //users: RDD[(VertexId, (String))],
                                                kmin: Int,
                                                displayResult: Boolean): Graph[Int, ED] = {

    // Graph[(Int, Boolean), ED] - boolean indicates whether it is active or not
    var g = graph.cache().outerJoinVertices(graph.degrees)((vid, oldData, newData) => newData.getOrElse(0)).cache()

    println(color("\nCall KCoreDecomposition", RED))

    g = computeCurrentKCore(g, kmin).cache()

    val v = g.vertices.filter { case (vid, vd) => vd >= kmin }.cache()

    // Display informations
    if (displayResult) {
      val degrees = graph.degrees
      val numVertices = degrees.count()
      val testK = kmin
      val vCount = g.vertices.filter { case (vid, vd) => vd >= kmin }.count()
      val eCount = g.triplets.map { t => t.srcAttr >= testK && t.dstAttr >= testK }.count()

      logWarning(s"Number of vertices: $numVertices")
      logWarning(s"Degree sample: ${degrees.take(10).mkString(", ")}")
      logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).collect().mkString(", "))
      logWarning(s"Degree distribution: " + degrees.map { case (vid, data) => (data, 1) }.reduceByKey(_ + _).take(10).mkString(", "))
      logWarning(s"K=$kmin, V=$vCount, E=$eCount")
    }

    g.subgraph(vpred = (vid, vd) => vd >= kmin)

//    // Create new RDD users
//    val newUser = users.join(v).map {
//      case (id, (username, rank)) => (id, username)
//    }
//
//    // Create a new graph
//    val gra = Graph(newUser, g.edges)
//
//    // Remove missing vertices as well as the edges to connected to them
//    gra.subgraph(vpred = (id, username) => username != null).cache()
  }

  def computeCurrentKCore[ED: ClassTag](graph: Graph[Int, ED], k: Int) = {
    println("Computing kcore for k=" + k)
    def sendMsg(et: EdgeTriplet[Int, ED]): Iterator[(VertexId, Int)] = {
      if (et.srcAttr < 0 || et.dstAttr < 0) {
        // if either vertex has already been turned off we do nothing
        Iterator.empty
      } else if (et.srcAttr < k && et.dstAttr < k) {
        // tell both vertices to turn off but don't need change count value
        Iterator((et.srcId, -1), (et.dstId, -1))

      } else if (et.srcAttr < k) {
        // if src is being pruned, tell dst to subtract from vertex count
        Iterator((et.srcId, -1), (et.dstId, 1))

      } else if (et.dstAttr < k) {
        // if dst is being pruned, tell src to subtract from vertex count
        Iterator((et.dstId, -1), (et.srcId, 1))

      } else {
        Iterator.empty
      }
    }

    // subtracts removed neighbors from neighbor count and tells vertex whether it was turned off or not
    def mergeMsg(m1: Int, m2: Int): Int = {
      if (m1 < 0 || m2 < 0) {
        -1
      } else {
        m1 + m2
      }
    }

    def vProg(vid: VertexId, data: Int, update: Int): Int = {
      if (update < 0) {
        // if the vertex has turned off, keep it turned off
        -1
      } else {
        // subtract the number of neighbors that have turned off this round from
        // the count of active vertices
        // TODO(crankshaw) can we ever have the case data < update?
        max(data - update, 0)
      }
    }

    // Note that initial message should have no effect
    Pregel(graph, 0, maxIterations = 100)(vProg, sendMsg, mergeMsg)
  }


  /**
    * @constructor time
    *
    *              timer for profiling block
    *
    * @param R $block - Block executed
    * @return Unit
    */
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + " seconds")
    result
  }

//  def subgraphCommunities(graph: Graph[String, String], users: RDD[(VertexId, (String))], displayResult: Boolean): (Array[Graph[String, String]], Array[Long]) = {
//
//    println(color("\nCall subgraphCommunities", RED))
//
//    // Find the connected components
//    val cc = time {
//      graph.connectedComponents().vertices.cache()
//    }
//
//    // Join the connected components with the usernames and id
//    // The result is an RDD not a Graph
//    val ccByUsername = users.join(cc).map {
//      case (id, (username, cci)) => (id, username, cci)
//    }.cache()
//
//    // Print the result
//    val lowerIDPerCommunity = ccByUsername.map { case (id, username, cci) => cci }.distinct().cache()
//
//    // Result will be stored in an array
//    //var result = new ArrayBuffer[Graph[String, String]]()
//    println("--------------------------")
//    println("Total community found: " + lowerIDPerCommunity.count())
//    println("--------------------------")
//
//
//    val collectIDsCommunity = lowerIDPerCommunity.collect()
//
//    val result = collectIDsCommunity.map(colID => Graph(ccByUsername.filter {
//      _._3 == colID
//    }.map { case (id, username, cc) => (id, username) }, graph.edges).subgraph(vpred = (id, username) => username != null).cache())
//
//    // Display communities
//    if (displayResult) {
//      println("\nCommunities found " + result.length)
//      for (community <- result) {
//        println("-----------------------")
//        community.edges.collect().foreach(println(_))
//        community.vertices.collect().foreach(println(_))
//      }
//    }
//
//    cc.unpersist()
//    lowerIDPerCommunity.unpersist()
//
//    (result, collectIDsCommunity)
//  }

  /**
    * getTriangleCount
    *
    * Compute the number of triangles passing through each vertex.
    *
    * @param Graph[String,String] $graph - Graph element
    * @param RDD[(VertexId, (String))] $users - Vertices
    * @return Unit
    *
    * @see [[org.apache.spark.graphx.lib.TriangleCount$#run]]
    */
//  def getTriangleCount(graph: Graph[String, String], users: RDD[(VertexId, (String))]): Unit = {
//
//    println(color("\nCall getTriangleCount", RED))
//
//    // Sort edges ID srcID < dstID
//    val edges = graph.edges.map { e =>
//      if (e.srcId < e.dstId) {
//        Edge(e.srcId, e.dstId, e.attr)
//      }
//      else {
//        Edge(e.dstId, e.srcId, e.attr)
//      }
//    }
//
//    // Temporary graph
//    val newGraph = Graph(users, edges, "").cache()
//
//    // Find the triangle count for each vertex
//    // TriangleCount requires the graph to be partitioned
//    val triCounts = newGraph.partitionBy(PartitionStrategy.RandomVertexCut).cache().triangleCount().vertices
//
//    val triCountByUsername = users.join(triCounts).map {
//      case (id, (username, rank)) => (id, username, rank)
//    }
//
//    println("Display triangle's sum for each user")
//    triCountByUsername.foreach(println)
//
//    println("\nTotal: " + triCountByUsername.map { case (id, username, rank) => rank }.distinct().count() + "\n")
//  }

  /**
    * @constructor ConnectedComponents
    *
    *              Compute the connected component membership of each vertex and return a graph with the vertex
    *              value containing the lowest vertex id in the connected component containing that vertex.
    *
    * @param Graph[String,String] $graph - Graph element
    * @param RDD[(VertexId, (String))] $users - Vertices
    * @return Unit
    *
    * @see [[org.apache.spark.graphx.lib.ConnectedComponents$#run]]
    */
//  def cc(graph: Graph[String, String], users: RDD[(VertexId, (String))]): Unit = {
//    println(color("\nCall ConnectedComponents", RED))
//
//    // Find the connected components
//    val cc = graph.connectedComponents().vertices
//
//    // Join the connected components with the usernames and id
//    val ccByUsername = users.join(cc).map {
//      case (id, (username, cc)) => (id, username, cc)
//    }
//    // Print the result
//    println(ccByUsername.collect().sortBy(_._3).mkString("\n"))
//
//    println("\nTotal groups: " + ccByUsername.map { case (id, username, cc) => cc }.distinct().count() + "\n")
//  }

  /**
    * @constructor StronglyConnectedComponents
    *
    *              Compute the strongly connected component (SCC) of each vertex and return a graph with the
    *              vertex value containing the lowest vertex id in the SCC containing that vertex.
    *
    *              Display edges's membership and total groups
    *
    * @param Graph[String,String] $graph - Graph element
    * @param Int $iteration - Number of iteration
    * @return Unit
    */
//  def scc(graph: Graph[String, String], iteration: Int): Unit = {
//
//    println(color("\nCall StronglyConnectedComponents : iteration : " + iteration, RED))
//    val sccGraph = graph.stronglyConnectedComponents(5)
//
//    val connectedGraph = sccGraph.vertices.map {
//      case (member, leaderGroup) => s"$member is in the group of $leaderGroup's edge"
//    }
//
//    val totalGroups = sccGraph.vertices.map {
//      case (member, leaderGroup) => leaderGroup
//    }
//
//    connectedGraph.collect().foreach(println)
//
//    println("\nTotal groups: " + totalGroups.distinct().count() + "\n")
//  }

  def color(str: String, col: String): String = "%s%s%s".format(col, str, ENDC)
}