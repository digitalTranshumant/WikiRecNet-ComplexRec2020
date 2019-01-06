package edu.ucu.wikidump

import org.apache.spark.sql.SparkSession

case class PageLink(from_ns: Int, from: Int, ns: Int, title: String)

case class Article(id: Int, ns: Int, title: String, isRedirect: Boolean = false)

case class Talk(talkId: Int, ns: Int, title: String)

case class Redirect(fromId: Int, ns: Int, redirectTitle: String, toId: Option[Int] = None)

case class CategoryLink(talkId: Int, categoryName: String)

object ArticleGraph {

  //  def countOccurrences(src: String, tgt: String): Int =
  //    src.sliding(tgt.length).count(window => window == tgt)

  case class Config(
                     pageLinksFile: String = "",
                     pagesFile: String = "",
                     redirectsFile: String = "",
                     categoriesFile: String = "",
                     output: String = "",
                     categoriesOutput: String = "",
                     withClassification: Boolean = false
                   )

  private val build = new scopt.OptionParser[Config]("ParseArticleGraph") {
    head("ParseArticleGraph", "1.0")
    help("convert sql dumps into graph edges")

    opt[String]("pagelinks").required().action((x, c) => c.copy(pageLinksFile = x))
    opt[String]("pages").required().action((x, c) => c.copy(pagesFile = x))
    opt[String]("redirects").required().action((x, c) => c.copy(redirectsFile = x))
    opt[String]("out").required().action((x, c) => c.copy(output = x))

    opt[Unit]("with-classification").action((_, c) => c.copy(withClassification = true))

    opt[String]("categories").action((x, c) => c.copy(categoriesFile = x))
    opt[String]("categories-out").action((x, c) => c.copy(categoriesOutput = x))

    checkConfig(c => if (!c.withClassification) success else if (c.categoriesFile.isEmpty || c.categoriesOutput.isEmpty)
      failure("for classification categories and categories-out must be provided")
    else success)
  }

  def main(args: Array[String]): Unit = {
    val config = build.parse(args, Config()).get

    val spark = SparkSession.builder()
      .appName("ReadArticleGraph").getOrCreate()

    import spark.implicits._

    val pagelinks = parseSqlDump(spark.read.textFile(config.pageLinksFile)).map {
      case from :: to_ns :: title :: from_ns :: nil =>
        PageLink(from_ns.toInt, from.toInt, to_ns.toInt, title)
    }.filter(l => l.from_ns == 0 && l.ns == 0)

    val articles = parseSqlDump(spark.read.textFile(config.pagesFile)).map {
      case id :: ns :: title :: _ :: _ :: isRedirect :: _ => Article(id.toInt, ns.toInt, title, isRedirect.toInt > 0)
    }.filter(_.ns == 0).cache()

    // option("lineSep", ";\n")

    val redirects = parseSqlDump(spark.read.textFile(config.redirectsFile)).map {
      case id :: ns :: title :: _ => Redirect(id.toInt, ns.toInt, title)
    }
      .filter(_.ns == 0)
      .join(articles, $"redirectTitle" === $"title").map(x =>
      Redirect(x.getInt(0), x.getInt(1), x.getString(2), Some(x.getAs[Int]("id")))
    )

    val titles = articles.filter(!_.isRedirect).union(
      articles
        .filter(_.isRedirect)
        .join(redirects, $"id" === $"fromId").map(x =>
        Article(x.getAs[Int]("toId"), x.getInt(1), x.getString(2), isRedirect = true)
      ))

    pagelinks.join(titles, Seq("title")).select("from", "id")
      .map { r => s"${r.getInt(0)}\t${r.getInt(1)}" }
      .rdd.saveAsTextFile(config.output)

    if (config.withClassification) {
      val talks = parseSqlDump(spark.read.textFile(config.pagesFile)).map {
        case id :: ns :: title :: _ => Talk(id.toInt, ns.toInt, title)
      }.filter(_.ns == 1)

      //.option("lineSep", ";\n")

      val categories = parseSqlDump(spark.read.textFile(config.categoriesFile)).map {
        case cl_from :: cl_to :: _ => CategoryLink(cl_from.toInt, cl_to)
      }.filter(c => c.categoryName.startsWith("WikiProject") && c.categoryName.endsWith("articles"))

      articles.join(talks, Seq("title")).join(categories, Seq("talkId")).select("id", "categoryName")
        .map { r => s"${r.getInt(0)}\t${r.getString(1)}" }
        .rdd.saveAsTextFile(config.categoriesOutput)
    }

  }
}
