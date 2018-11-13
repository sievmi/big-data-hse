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

    val bigramsRDD = wordsRDD.flatMap(word => {
      val charArray = word.toCharArray
      (1 until charArray.size - 1).map(idx => {
        s"${charArray(idx)}${charArray(idx + 1)}" -> 1
      })
    })

    val top10Bigramm = bigramsRDD.reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10)

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/task2")))
    top10Bigramm.zipWithIndex.foreach {
      case (idx, value) =>
        outputWriter.write(s"$idx. $value")
        outputWriter.write("\n")
    }
    outputWriter.flush()
    outputWriter.close()
  }

  def isEngLetter(letter: Character): Boolean = {
    letter >= 'a' && letter <= 'z' || letter >= 'A' && letter <= 'Z'
  }
}
