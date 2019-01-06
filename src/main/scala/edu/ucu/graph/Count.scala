package edu.ucu.graph

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object Count {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("yarn")
      .appName("counter").getOrCreate()

    val graph = GraphLoader.edgeListFile(spark.sparkContext, args(0))

    println(graph.vertices.count(), graph.edges.count())
  }
}
