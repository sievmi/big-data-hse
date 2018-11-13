/**
  * Created by sievmi on 13.11.18  
  */


import java.io.BufferedOutputStream

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkContext, _}

object SparkWordCount {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setMaster("yarn").setAppName("Wiki word count")
    val sc: SparkContext = new SparkContext(conf)

    val input = sc.textFile("hdfs://data/wiki/en/articles")


    val words = input.flatMap(line ⇒ line.split("\t").tail).flatMap(_.split(" "))
    val count = words.count()

    import org.apache.hadoop.fs.FileSystem
    // Hadoop Config is accessible from SparkContext// Hadoop Config is accessible from SparkContext

    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Output file can be created from file system.
    val output = fs.create(new Path("/user/esidorov/hw5_1.txt"))

    // But BufferedOutputStream must be used to output an actual text file.
    val os = new BufferedOutputStream(output)

    os.write(s"$count".getBytes("UTF-8"))
  }
}
