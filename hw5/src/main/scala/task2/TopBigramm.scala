package task2

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 13.11.18  
  */
object TopBigramm {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Count capital letters").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")

    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .map(line => line.filter(c => c.isLetter || c == ' '))
      .flatMap(_.split(" "))
      .filter(_.length >= 2)

    val bigramsRDD = wordsRDD.map(word => {
      val charArray = word.toCharArray
      (1 until charArray.size - 1).map(idx => {
        s"${charArray(idx)}${charArray(idx + 1)}" -> 1
      })
    })

    bigramsRDD.saveAsTextFile("hw5/task2")
  }
}
