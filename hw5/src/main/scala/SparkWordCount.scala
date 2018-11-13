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
    val pairsRDD = wordsRDD.map(str => {
      str.charAt(0) -> (if (Character.isUpperCase(str.charAt(0))) 1 else 0)
    })
    pairsRDD.reduceByKey(_ + _).saveAsTextFile("/users/esidorov/hw5_1.txt")
  }
}
