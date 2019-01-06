package edu.ucu.wikidump

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object CrossLingualMapping {

  case class Config(
                     fromLanguage: String = "enwiki",
                     toLanguage: String = "ukwiki",
                     fromPagesFile: String = "",
                     toPagesFile: String = "",
                     wikidataFile: String = "",
                     output: String = "",
                     debug: Boolean = false
                   )

  private val build = new scopt.OptionParser[Config]("CreateCrossLingualMapping") {
    head("CrossLingualMapping", "1.0")
    help("convert title <-> title mapping into articleId <-> articleId")

    opt[String]("from").required()
      .action((x, c) => c.copy(fromLanguage = x))
      .validate(x => if (x.endsWith("wiki")) success else failure("language must ends with 'wiki'"))

    opt[String]("to").required()
      .action((x, c) => c.copy(toLanguage = x))
      .validate(x => if (x.endsWith("wiki")) success else failure("language must ends with 'wiki'"))

    opt[String]("from-pages").required().action((x, c) => c.copy(fromPagesFile = x))
    opt[String]("to-pages").required().action((x, c) => c.copy(toPagesFile = x))

    opt[String]("wikidata").required().action((x, c) => c.copy(wikidataFile = x))
    opt[String]("output").required().action((x, c) => c.copy(output = x))

    opt[Unit]("debug").action((_, c) => c.copy(debug = true))
  }

  val cleanTitle: String => String = Option(_)
    .getOrElse("").replace("'", "").replace("(", "").replace(")", "").replace(",", "")
    .replace(" ", "_")

  val isValidTitle: String => Boolean = !_.map(_.isDigit).exists(x => x)

  val cleanTitleUDF: UserDefinedFunction = udf(cleanTitle)
  val isValidTitleUDF: UserDefinedFunction = udf(isValidTitle)


  def main(args: Array[String]): Unit = {
    val config = build.parse(args, Config()).get

    val spark = SparkSession.builder() //.master("yarn")
      .appName("CrossLingualMapping").getOrCreate()

    import spark.implicits._

    val leftArticles = parseSqlDump(spark.read.textFile(config.fromPagesFile)).map {
      case id :: ns :: title :: _ :: _ :: isRedirect :: _ => Article(id.toInt, ns.toInt, title, isRedirect.toInt > 0)
    }
      .filter(_.ns == 0)
      .filter(!_.isRedirect)
      .select($"id".as("leftId"), $"title".as("left"))

    val rightArticles = parseSqlDump(spark.read.textFile(config.toPagesFile)).map {
      case id :: ns :: title :: _ :: _ :: isRedirect :: _ => Article(id.toInt, ns.toInt, title, isRedirect.toInt > 0)
    }
      .filter(_.ns == 0)
      .filter(!_.isRedirect)
      .select($"id".as("rightId"), $"title".as("right"))


    val mapping = spark.read.json(config.wikidataFile)
      .select(
        cleanTitleUDF(col(config.fromLanguage)).as("left"),
        cleanTitleUDF(col(config.toLanguage)).as("right"),
        isValidTitleUDF(col(config.fromLanguage)).as("isValidLeft"),
        isValidTitleUDF(col(config.toLanguage)).as("isValidRight"))
      .where(
        $"left" =!= "" && $"right" =!= ""
          && $"isValidLeft" && $"isValidRight" // additional cleaning (may be optional)
      )

    if (config.debug) {
      mapping.show()
      rightArticles.show()
      println(mapping.count(), rightArticles.count())
      mapping.join(rightArticles, Seq("right"), "outer").where($"rightId".isNull).sample(0.1).show(200)
    }

    mapping
      .join(leftArticles, "left")
      .join(rightArticles, "right")
      .map { r =>
        s"${r.getAs[Int]("leftId")} ${r.getAs[Int]("rightId")}"
      }.rdd.saveAsTextFile(config.output)
  }
}
