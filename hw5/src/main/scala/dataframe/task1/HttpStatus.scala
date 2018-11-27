package dataframe.task1

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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

    val ansDF = inputDF.select("status", "ip").filter(!_.anyNull)
      .groupBy("status").agg(countDistinct("ip")).as("count")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("./hw5/dataframes/task1")))

    outputWriter.println(inputDF.columns.mkString(","))
    outputWriter.println()
    ansDF.collect().foreach(outputWriter.println)
    outputWriter.flush()
    outputWriter.close()
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

}
