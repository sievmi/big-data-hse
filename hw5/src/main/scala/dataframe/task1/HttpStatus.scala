package dataframe.task1

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by sievmi on 16.11.18  
  */
object HttpStatus {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("HTTP status").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val inputDF = sqlContext.read//.format("tsv")
      .option("delimiter", "\t")
      .load("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/dataframes/task1")))
    outputWriter.println(inputDF.columns.mkString(","))
    outputWriter.println()
    outputWriter.println(inputDF.head().toString())
    outputWriter.flush()
    outputWriter.close()
  }
}
