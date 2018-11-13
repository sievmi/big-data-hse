package task3

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 13.11.18  
  */
object StopWordsCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Count capital letters").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("/data/wiki/en/articles")

    val stopWordsAccums = sc.broadcast(Map("and" -> sc.longAccumulator("and accumulator"), "the" -> sc.longAccumulator("the accumulator")))

    val wordsRDD = input.flatMap(line ⇒ line.split("\t").tail)
      .flatMap(_.split(" "))

    wordsRDD.map(word => {
      if (stopWordsAccums.value.contains(word)) {
        stopWordsAccums.value(word).add(1)
      }
    })

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/task3")))
    outputWriter.write("and - ")
    outputWriter.write(s"${stopWordsAccums.value("and").value}")
    outputWriter.write("the - ")
    outputWriter.write(s"${stopWordsAccums.value("the").value}")
    outputWriter.flush()
    outputWriter.close()
  }
}
