/**
  * Created by sievmi on 13.11.18  
  */


import java.io.{BufferedOutputStream, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkContext, _}

object SparkWordCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Wordcount").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")


    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .flatMap(_.split(" ")).filter(str => str.nonEmpty && isCapitalLetter(str.charAt(0)))

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5_1")))
    outputWriter.println(wordsRDD.count())
    outputWriter.flush()
    outputWriter.close()
  }


  private def isCapitalLetter(letter: Char): Boolean = {
    letter >= 'A' && letter <= 'Z'
  }
}
