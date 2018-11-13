package task1

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkContext, _}

/**
  * Created by sievmi on 13.11.18
  */


object SparkWordCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Count capital letters").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")

    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .flatMap(_.split(" "))

    val count = wordsRDD.filter(str => str.nonEmpty && isCapitalLetter(str.charAt(0)))

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/task1")))
    outputWriter.println(count)
    outputWriter.flush()
    outputWriter.close()
  }

  private def isCapitalLetter(letter: Char): Boolean = {
    letter >= 'A' && letter <= 'Z'
  }
}
