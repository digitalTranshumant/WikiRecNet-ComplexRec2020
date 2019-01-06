package edu.ucu.kcore

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import ml.sparkling.graph.operators.OperatorsDSL._
import ml.sparkling.graph.operators.algorithms.community.pscan.PSCAN
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.sql.SparkSession


object KCore extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("yarn")
      .appName("counter").getOrCreate()

    val g = GraphLoader.edgeListFile(spark.sparkContext, args(0)
      //edgeStorageLevel = StorageLevel.MEMORY_AND_DISK,  vertexStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    val minK = args(1).toInt
    //val communities = PSCAN.detectCommunities(g).vertices
    val communities  = CommunityUtils.getKCoreGraph(g, minK, displayResult = true)
      .stronglyConnectedComponents(numIter = 30).vertices
    //val communities = LabelPropagation.run(g, 50).vertices
    val communitiesNum = communities.map(_._2).countByValueApprox(300)

    logWarning(s"Number of communities $communitiesNum")

    if (args.length > 2) {
      communities.saveAsTextFile(args(2))
    }

    spark.stop()
  }

}
