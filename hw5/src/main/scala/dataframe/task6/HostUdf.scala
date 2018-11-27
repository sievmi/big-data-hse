package dataframe.task6

import java.net.URI

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 16.11.18  
  */
object HostUdf {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("IpUdf").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val sepHost = (url: String) => new URI(url).getHost
    val hostUdf = udf(sepHost)

    val schema = Encoders.product[UserLog].schema
    val inputDF = sqlContext.read.option("sep", "\t").schema(schema)
      .csv("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")


    val ansDf = inputDF.withColumn("host", hostUdf(col("url")))


    ansDf.rdd.saveAsTextFile("./hw5/dataframes/task6")
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

}
