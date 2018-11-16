package dataframe.task1

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions.countDistinct

/**
  * Created by sievmi on 16.11.18  
  */
object HttpStatus {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("HTTP status").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val schema = Encoders.product[UserLog].schema
    val inputDF = sqlContext.read.option("sep", "\t").schema(schema)
      .csv("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")

    val selected = inputDF.select("status", "ip").filter(!_.anyNull).groupBy("status").agg(countDistinct("ip")).as("count")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/dataframes/task1")))
    outputWriter.println(inputDF.columns.mkString(","))
    outputWriter.println()
    selected.head(3).foreach(outputWriter.println)
    outputWriter.flush()
    outputWriter.close()
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String, c5: String, status: String, browser: String)

}
