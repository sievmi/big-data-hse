package rdd.task3

import java.io.{File, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by sievmi on 13.11.18  
  */
object StopWordsCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Count capital letters").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")

    val stopWordsFile = new File("/home/pakhtyamov/stop_words_en.txt")
    val stopWordsAccums = Source.fromFile(stopWordsFile).getLines().map(line => {
      val word = line.trim
      word -> sc.longAccumulator(word)
    }).toMap

    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .flatMap(_.split(" "))

    wordsRDD.map(word => {
      if (stopWordsAccums.contains(word)) {
        stopWordsAccums(word).add(1)
      }
    }).count()

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("./hw5/rdd/task3")))
    stopWordsAccums.foreach {
      case (key, value) =>
        outputWriter.write(s"$key ${value.value}")
        outputWriter.write("\n")
    }
    outputWriter.flush()
    outputWriter.close()
  }
}
