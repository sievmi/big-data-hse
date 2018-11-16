package dataframe.task3

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 16.11.18  
  */
object Regions {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Regions").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val logsSchema = Encoders.product[UserLog].schema
    val rowLogsDF = sqlContext.read.option("sep", "\t").schema(logsSchema)
      .csv("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")

    val regionSchema = Encoders.product[Ip2Region].schema
    val regionsDF = sqlContext.read.option("sep", "\t").schema(regionSchema)
      .csv("/user/pakhtyamov/data/user_logs/ip_data_M/ipDataM.txt")

    val ansDF = rowLogsDF.join(regionsDF, "ip").select("region", "ip").groupBy("region")
      .agg(countDistinct("ip")).as("count")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/dataframes/task3")))

    outputWriter.println(ansDF.columns.mkString(","))
    outputWriter.println()
    ansDF.collect().foreach(outputWriter.println)
    outputWriter.flush()
    outputWriter.close()


  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

  case class Ip2Region(ip: String, region: String)

}
