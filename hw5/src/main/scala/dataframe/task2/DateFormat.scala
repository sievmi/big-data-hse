package dataframe.task2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{date_format, col, to_date}

/**
  * Created by sievmi on 16.11.18  
  */
object DateFormat {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Date format").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val schema = Encoders.product[UserLog].schema
    val inputDF = sqlContext.read.option("sep", "\t").schema(schema)
      .csv("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")

    val formattedDF = inputDF.select("ip", "date", "url", "c5", "status", "browser")
      .withColumn("date", date_format(to_date(col("date"), "yyyyMMddHHmmss"), "yy-MM-dd"))

    formattedDF.rdd.saveAsTextFile("./hw5/dataframes/task2")
  }

  case class UserLog(ip: String, c1: String, c2: String, date: String, url: String,
                     c5: String, status: String, browser: String)

}
