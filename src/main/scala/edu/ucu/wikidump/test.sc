import java.io.StringReader

def countOccurrences(src: String, tgt: String): Int =
  src.sliding(tgt.length).count(window => window == tgt)


//val s = "(2693059,0,'\\'hours...\\'',0),(2832650,0,'\\'hours...\\'',0),(4612396,0,'\\'s-Gravenpolder_(dorp)',0),(279028,0,'\\'s-Hertogenbosch',0),(2084952,0,'\\'s-Hertogenbosch',0),(4138013,0,'\\'s-Hertogenbosch',0),(6855822,0,'\\'t_Hart',0),(6857715,0,'\\'t_Hart',0),(2619699,0,'\\'t_Is_OK',0),(7443937,0,'\\'t_Is_OK',0),(1579562,0,'\\'t_Pallieterke',0),(3328103,0,'\\'t_Vrije_Schaep',0),(4704603,0,'\\'te',0),(7250206,0,'\\'Абд_Аллах_\\'Афиф_ад-Дин-Эфенди',0),(6284817,0,'\\'Абдаллах_ибн_Ахмадом_ан-Насафи',0),(1147415,0,'\\'Айн',0),(5276842,0,'\\'Айя',0),(260654,0,'\\'Жур_фикс\\'',0),(2053170,0,'\\'Каратканск',0),(6509016,0,'\\'Построй_дом,_посади_дерево',0),(6283401,0,'\\'исма',0),(1778508,0,'\\'т_Хантье_(Нидерланды)',0),(3187537,0,'\\'т_Харде_(футбольный_клуб)',0),(90451,0,'\\'т_Хоофт,_Герард',0),(2112954,0,'\\'т_Хоофт,_Герард',0),(2204991,0,'\\'т_Хоофт,_Герард',0),(2754859,0,'\\'т_Хоофт,_Герард',0),(4336794,0,'\\'цис-транс-изомерия',0),(37095,0,'(',0),(47186,0,'(',0),(47907,0,'(',0),(51052,0,'(',0),(83090,0,'(',0),(83928,0,'(',0),(99081,0,'(',0),(133450,0,'(',0),(721722,0,'(',0),(1358746,0,'(',0)"
val s = "(6576080,0,'&),(RQ',0),(7046093,0,'(ABC),(DEF)\\',&RQ',0),(7071275,0,'&RQ',0),(7404602,0,'&RQ',0),(7424470,0,'&RQ',0),(631537,0,'&_(сингл_Аюми_Хамасаки)',0),(1455572,0,'&_(сингл_Аюми_Хамасаки)',0),(1365214,0,'&_Buhse',0),(979472,0,'&_YOU_revolution',0),(83090,0,'\\'',0),(2230539,0,'\\'',0),(4041883,0,'\\'',0),(6128843,0,'\\'',0),(6473035,0,'\\'\\'\\\"trionfi\\\"\\'\\'',0),(1883103,0,'\\'\\'\\'\\\"Маяк\\\",_\\\"Турбина\\\",_\\\"Авиатор-ХАИ\\\",_\\\"Ритм\\\",_\\\"Газовик-ХГД\\\",_\\\"Электротяжмаш\\\"\\'\\'\\'_Харьков',0),(4173576,0,'\\'\\'\\'1782\\'\\'\\'_год,_как_Русское_село_1823_года',0),(157926,0,'\\'\\'\\'AN/AAR_56\\'\\'\\'',0),(157926,0,'\\'\\'\\'AN/ALR-94\\'\\'\\'',0),(5610120,0,'\\'\\'\\'B\\'\\'\\'elted_Case',0),(918841,0,'\\'\\'\\'Chamberlain\\'\\'\\'',0)"

val reader = new StringReader(s)

val lines = s.split("\\),\\(").foldRight(List.empty[String]) {
  case (el, acc) => if (countOccurrences(el, "'") % 2 == 1 && countOccurrences(acc.head, "'") % 2 == 1)
    el ++ acc.head :: acc.tail
  else el :: acc
}
lines.mkString("\n")


import com.univocity.parsers.csv.{CsvFormat, CsvParser, CsvParserSettings}

val format = new CsvFormat
format.setQuote(''')
format.setQuoteEscape('\\')

val settings = new CsvParserSettings
settings.setFormat(format)

val parser = new CsvParser(settings)

import collection.JavaConverters._

val res = parser.parseAll(reader)
res.asScala.map(_.toList)

//case class FilterState(s: List[Char] = List.empty, inParentheses: Boolean = false)

//def filterLine(line: String): String = {
//  line.replace("\\'", "")
//    .foldRight(("".toList, false)) ((char, state) =>
//      (state, char) match {
//        case ((acc, inQuotes), ''') => (''' :: acc, !inQuotes)
//        case ((acc, inQuotes), c) => (if (Array('(', ')').contains(c) && inQuotes) acc else c :: acc, inQuotes)
//
//      })._1.mkString("")
//}
//
//filterLine(s)
//
