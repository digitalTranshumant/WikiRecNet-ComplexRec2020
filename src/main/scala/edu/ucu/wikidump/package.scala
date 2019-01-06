package edu.ucu

import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}
import org.apache.spark.sql.Dataset

package object wikidump {

  def filterLine(line: String): String = {
    line.replace("\\\\", "").replace("\\'", "")
      .foldRight(("".toList, false))((char, state) =>
        (state, char) match {

          case ((acc, inQuotes), ''') => //if (!inQuotes && acc.head != ',') println(acc.mkString("").substring(0, acc.length.min(20)))
            (''' :: acc, !inQuotes)
          case ((acc, inQuotes), c) => (if (inQuotes && (c == '(' || c == ')' || c == ',')) acc else c :: acc, inQuotes)

        })._1.mkString("")
  }

  def parseSqlDump(lines: Dataset[String]): Dataset[List[String]] = {

    val l = lines.filter(_.startsWith("INSERT"))
      .map(x => x.substring(x.indexOf("VALUES") + "VALUES (".length, x.length - 2))
      .flatMap(filterLine(_).split("\\),\\("))

    l.mapPartitions(partition => {
      val format = new CsvFormat
      format.setQuote(''')
      format.setQuoteEscape('\\')

      val settings = new CsvParserSettings
      settings.setFormat(format)

      val parser = new CsvParser(settings)

      partition.map { x =>
        parser.parseLine(x).toList
      }
    })
  }
}
