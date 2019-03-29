package edu.ucu.wikidump

import java.util.{Calendar, GregorianCalendar}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, lower}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


//case class Revision(userId: Int, pageId: Int, pageTitle: String, timestamp: Int)
//
//case class Result(userId: Int, pageIds: List[Int])
//
//object RevisionStats {
//
//  case class Config(
//                     minDate: Calendar = new GregorianCalendar(2015, 1, 1),
//                     minBytes: Int = 50,
//                     takeMinor: Boolean = false,
//                     revisionsFile: String = ""
//                   )
//
//  private val build = new scopt.OptionParser[Config]("ParseArticleGraph") {
//    help("Analyze filtered revisions")
//    head("RevisionStats", "1.0")
//
//    opt[Calendar]("min-date").action((x, c) => c.copy(minDate = x))
//    opt[Int]("min-bytes").action((x, c) => c.copy(minBytes = x))
//    opt[Unit]("minor").action((_, c) => c.copy(takeMinor = true))
//
//    opt[String]("revisions").required().action((x, c) => c.copy(revisionsFile = x))
//  }
//
//  def main(args: Array[String]): Unit = {
//    val config = build.parse(args, Config()).get
//
//    val spark = SparkSession.builder() //.master("yarn")
//      .appName("RevisionStats").getOrCreate()
//
//    import spark.implicits._
//
//    val schema = StructType(Array(
//      StructField("userId", IntegerType, nullable = true),
//      StructField("userName", StringType, nullable = false),
//      StructField("pageId", IntegerType, nullable = false),
//      StructField("pageTitle", StringType, nullable = false),
//      StructField("minor", StringType, nullable = false),
//      StructField("comment", StringType),
//      StructField("bytes", IntegerType),
//      StructField("timestamp", IntegerType)
//    ))
//
//    val revisions = spark.read.option("sep", "\t").schema(schema).csv(config.revisionsFile)
//
//    val revFiltered = revisions.withColumn("userNameL", lower($"userName")).where(
//      $"userId" =!= 0 &&
//        $"minor" === config.takeMinor &&
//        !$"userNameL".contains("bot") &&
//        $"timestamp" > (config.minDate.getTimeInMillis / 1000) &&
//        $"bytes" >= config.minBytes
//    ).as[Revision]
//
//    revFiltered
//      .groupByKey(r => r.userId)
//      .agg(countDistinct('pageId).as[Long])
//      .groupByKey(_._2).count()
//      .filter(_._1 < 500).collect.foreach(println)
//
//  }
//}
