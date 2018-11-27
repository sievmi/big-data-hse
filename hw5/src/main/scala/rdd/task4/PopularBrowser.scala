package rdd.task4

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 14.11.18  
  */
object PopularBrowser {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Popular browsers").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val logsInputRDD = sc.textFile("/user/pakhtyamov/big_log_10000/")
    val ip2BrowserRDD = logsInputRDD.flatMap(parserRowLogsLine)

    val inputIpLookupRDD = sc.textFile("/user/pakhtyamov/geoiplookup_10000/")
    val ip2CounryRDD = inputIpLookupRDD.flatMap(parseRowGeoIpLine)

    val country2BrowserRDD = ip2CounryRDD.join(ip2BrowserRDD)
      .reduceByKey((first, second) => first._1 -> first._2)
      .map(_._2)

    val topBrowsersRDD = country2BrowserRDD.groupByKey().map {
      case (country, browsers) =>
        val browsersCount = browsers.groupBy(s => s).map(d => d._1 -> d._2.size).toSeq.sortBy(_._2)
        country -> browsersCount.takeRight(3).reverse.mkString("; ")
    }

    topBrowsersRDD.saveAsTextFile("./hw5/rdd/task4")
  }

  def parseRowGeoIpLine(line: String): Option[(String, String)] = {
    val arr = line.split(",")
    val ipOpt = arr.headOption.map(_.filter(c => c != '\"'))
    val countryOpt = arr.lastOption.map(_.filter(c => c != '\"'))

    for {
      ip <- ipOpt
      country <- countryOpt
    } yield ip -> country
  }

  def parserRowLogsLine(line: String): Option[(String, String)] = {
    pattern.findFirstMatchIn(line).flatMap(m => {
      val arr = m.subgroups
      if (arr.size >= 6) {
        val ip = arr.head
        val browser = {
          val browsersString = arr(6).toLowerCase
          if (browsersString.contains("chrome")) {
            "Chrome"
          } else if (browsersString.contains("firefox")) {
            "Firefox"
          } else if (browsersString.contains("safari")) {
            "Safari"
          } else if (browsersString.contains("msie") || browsersString.contains("iemobile")) {
            "Internet explorer"
          } else "unknown"
        }

        Some(ip -> browser)
      } else None
    })
  }

  private val pattern =
    """([(\d\.)]+) - - \[(.?)*\] ".+ (.+) .+" (\d+) (\d+) "(.+)" "(.*?)"""".r

  case class Data(ip: String, browser: String)

  case class GeoIP(ip: String, country: String)

}
