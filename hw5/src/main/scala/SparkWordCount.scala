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


    val words = input.flatMap(line ⇒ line.split("\t").tail).flatMap(_.split(" "))
    val count = words.count()

    /*val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path("/user/esidorov/hw5_1.txt"))
    val os = new BufferedOutputStream(output)
    os.write(s"$count".getBytes("UTF-8"))*/
  }
}
