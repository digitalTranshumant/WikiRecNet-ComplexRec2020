package edu.ucu.wikidump

import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, lower}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


case class Revision(userId: Int, pageId: Int, pageTitle: String, timestamp: Int)

case class Result(userId: Int, pageIds: List[Int])

object Revision {

  case class Config(
                     minDate: Calendar = new GregorianCalendar(2015, 1, 1),
                     minBytes: Int = 50,
                     maxEditions: Int = 1000,
                     takeMinor: Boolean = false,
                     revisionsFile: String = "",
                     output: String = "",
                     onlyUniqueEditions: Boolean = false
                   )

  private val build = new scopt.OptionParser[Config]("ParseArticleGraph") {
    help("Group revisions by user")
    head("Revisions", "1.0")

    opt[Calendar]("min-date").action((x, c) => c.copy(minDate = x))
    opt[Int]("min-bytes").action((x, c) => c.copy(minBytes = x))
    opt[Unit]("minor").action((_, c) => c.copy(takeMinor = true))

    opt[String]("revisions").required().action((x, c) => c.copy(revisionsFile = x))
    opt[Unit]("unique").action((_, c) => c.copy(onlyUniqueEditions = true))
    opt[Int]("max-editions").action((x, c) => c.copy(maxEditions = x))
  }

  def main(args: Array[String]): Unit = {
    val config = build.parse(args, Config()).get

    val spark = SparkSession.builder() //.master("yarn")
      .appName("revision").getOrCreate()

    import spark.implicits._

    val schema = StructType(Array(
      StructField("userId", IntegerType, nullable = true),
      StructField("userName", StringType, nullable = false),
      StructField("pageId", IntegerType, nullable = false),
      StructField("pageTitle", StringType, nullable = false),
      StructField("minor", StringType, nullable = false),
      StructField("comment", StringType),
      StructField("bytes", IntegerType),
      StructField("timestamp", IntegerType)
    ))

    val revisions = spark.read.option("sep", "\t").schema(schema).csv(config.revisionsFile)

    val revFiltered = revisions.withColumn("userNameL", lower($"userName")).where(
      $"userId" =!= 0 &&
        $"minor" === config.takeMinor &&
        !$"userNameL".contains("bot") &&
        $"timestamp" > (config.minDate.getTimeInMillis / 1000) &&
        $"bytes" >= config.minBytes
    ).as[Revision]

    println("Input revisions", revFiltered.count)

    val bots = revFiltered.groupByKey(r => r.userId).agg(countDistinct('pageId).as[Long]).filter {
      _._2 > config.maxEditions
    }.map {
      case (userId, _) => userId

    }.rdd.collect()

    val botsBC = spark.sparkContext.broadcast(bots)

    println("Bots detected", bots.length)

    val out = revFiltered.rdd
      .filter((r: Revision) => !botsBC.value.contains(r.userId))
      .map((r: Revision) => (r.userId, r)).groupByKey().flatMap {
      case (user_id, revs) =>
        val revsOrdered = revs.toList.sortBy(_.timestamp)
        val page_ids = revsOrdered.map(_.pageId).distinct
        val page_titles = revsOrdered.map(_.pageTitle).distinct

        if (!config.onlyUniqueEditions) {
          Some(Result(user_id, page_ids))
        } else {

          if (page_ids.length != page_titles.length) {
            None
          } else if (revsOrdered.length - page_ids.length > 20) {
            None
          } else {
            Some(Result(user_id, page_ids))
          }
        }

    }.toDS

    out.write.json(config.output)

    //    val w1 = Window
    //      .partitionBy('user_id)
    //      .orderBy('timestamp)
    //      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    //
    //    revFiltered
    //      .withColumn("all_edits", collect_set('page_id).over(w1))
    //      .withColumn("all_titles", collect_set('page_title).over(w1))
    //      .withColumn("try_count", count('page_id).over(w1))
    //      .withColumn("all_edits_len", size('all_edits))
    //      .where('all_edits_len between(10, 200))
    //
    //
    //      .withColumn("delta", abs('try_count - 'all_edits_len))
    //      .where('delta < 10)
    //
    //
    //      .withColumn("all_titles_len", size('all_titles))
    //
    //      .where('all_titles_len === 'all_edits_len)
    //      .select('user_id, 'all_edits).distinct().write.json(args(1))


  }
}
