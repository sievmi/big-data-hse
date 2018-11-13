/**
  * Created by sievmi on 13.11.18  
  */


import java.io.BufferedOutputStream

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkContext, _}

object SparkWordCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Wordcount").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")


    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .flatMap(_.split(" "))
    val pairsRDD = wordsRDD.flatMap(str => {
      if (str.length > 0 && isCapitalLetter(str.charAt(0))) Some(str.charAt(0) -> 1) else None
    })
    pairsRDD.reduceByKey(_ + _).saveAsTextFile("/user/esidorov/hw5_1")
  }


  private def isCapitalLetter(letter: Char): Boolean = {
    letter >= 'A' && letter <= 'Z'
  }
}
