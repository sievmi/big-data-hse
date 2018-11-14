package task4

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 14.11.18  
  */
object PopularBrowser {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Popular browsers").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val inputRDD = sc.textFile("/user/pakhtyamov/big_log_10000/")
    val parserdLogRDD = inputRDD.flatMap(parserRowLine)


    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/task4")))
    parserdLogRDD.take(20).foreach(s => {
      outputWriter.write(s.toString)
      outputWriter.write("\n")
    })
    outputWriter.flush()
    outputWriter.close()
  }

  def parserRowLine(line: String): Option[Data] = {
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

        Some(Data(ip, browser))
      } else None
    })
  }

  private val pattern =
    """([(\d\.)]+) - - \[(.?)*\] ".+ (.+) .+" (\d+) (\d+) "(.+)" "(.*?)"""".r

  case class Data(ip: String, browser: String)

}
