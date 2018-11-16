package dataframe.task1

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SQLContext, SparkSession}
import org.apache.spark.sql.functions.{countDistinct, count}

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

    val selected = inputDF.select("status", "ip")
    val filteredSelected = selected.filter(!_.anyNull)
    val ansDfDistinct = filteredSelected.groupBy("status").agg(countDistinct("ip")).as("count")
    val ans = filteredSelected.groupBy("status").agg(count("ip")).as("count")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/dataframes/task1")))
    outputWriter.println(s"Row size = ${inputDF.collect().length}")
    outputWriter.println(s"Selected size = ${selected.collect().length}")
    outputWriter.println(s"Selected filtered size = ${filteredSelected.collect().length}")
    outputWriter.println(s"Ans distinct size = ${ansDfDistinct.collect().length}")
    outputWriter.println(s"Ans size = ${ans.collect().size}")

    outputWriter.println(inputDF.columns.mkString(","))
    outputWriter.println()
    ansDfDistinct.collect().foreach(outputWriter.println)
    outputWriter.flush()
    outputWriter.close()
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String, c5: String, status: String, browser: String)

}
