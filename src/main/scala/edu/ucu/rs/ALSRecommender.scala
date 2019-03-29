package edu.ucu.rs

import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, explode, lit}


object ALSRecommender {

  case class Config(
                     sessionFilesPattern: String = "",
                     master: String = "local[*]",
                     outputUsers: String = "",
                     outputPages: String = "",
                     rank: Int = 10
                   )

  private val build = new scopt.OptionParser[Config]("ALSRecommender") {
    head("ALSRecommender", "1.0")

    opt[String]("input").required().action((x, c) => c.copy(sessionFilesPattern = x))
    opt[String]("users").required().action((x, c) => c.copy(outputUsers = x))
    opt[String]("pages").required().action((x, c) => c.copy(outputPages = x))
    opt[String]("master").action((x, c) => c.copy(master = x))
    opt[Int]("dim").action((x, c) => c.copy(rank = x))
  }

  def main(args: Array[String]): Unit = {
    val config = build.parse(args, Config()).get

    val spark = SparkSession.builder().master(config.master)
      .appName("ALSRecommender").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val input = spark.read.json(config.sessionFilesPattern)

    val data = input.select('userId, explode('pageIds).as("pageId"), lit(1.0).as("rating")).distinct

    val Array(training, test) = data.randomSplit(Array(0.9, 0.1))


    val als = new ALS()
      .setImplicitPrefs(true)
      .setUserCol("userId")
      .setItemCol("pageId")
      .setRatingCol("rating")
      .setMaxIter(5)
      .setRank(config.rank)

    //training.show()
    //training.write.json("test/")
    val model = als.fit(training)

    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val userRecommended = model.recommendForUserSubset(test, 10)
      .select($"userId", $"recommendations.pageId".as("recommendation"))
    //      .map { case Row(user: Int, recs: Array[(Int, Float)]) =>
    //      Row(user, recs.map { case (item, rating) => item })
    //    }

    userRecommended.show()

    val testByUser = test.groupByKey(_.getAs[Long]("userId"))
      .agg(collect_list("pageId").as[Array[Long]]).toDF("userId", "actual")


    val relevant = userRecommended.join(testByUser, "userId")
      .select("recommendation", "actual").as[(Array[Long], Array[Long])]

    relevant.show()
    //      .map {
    //      case (user: Int, (actual: Array[Int], predictions: Array[Rating[Int]])) =>
    //        (predictions.map(_.item), actual)
    //
    //    }

    val metrics = new RankingMetrics(relevant.rdd)

    // Precision at K
    Array(5, 10, 20).foreach { k =>
      println(s"Precision at $k = ${metrics.precisionAt(k)}")
    }

    // Mean average precision
    println(s"Mean average precision = ${metrics.meanAveragePrecision}")

    // Normalized discounted cumulative gain
    Array(5, 10, 20).foreach { k =>
      println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
    }

    model.userFactors.rdd.map {
      row =>
        s"${row.getInt(0)} ${row.getList[Float](1).toArray.mkString(" ")}"
    }.saveAsTextFile(config.outputUsers)
    model.itemFactors.rdd.map {
      row =>
        s"${row.getInt(0)} ${row.getList[Float](1).toArray.mkString(" ")}"
    }.saveAsTextFile(config.outputPages)
  }
}
