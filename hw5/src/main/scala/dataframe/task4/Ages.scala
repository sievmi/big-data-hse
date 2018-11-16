package dataframe.task4

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.countDistinct

/**
  * Created by sievmi on 16.11.18  
  */
object Ages {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("Ages").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val logsSchema = Encoders.product[UserLog].schema
    val rowLogsDF = sqlContext.read.option("sep", "\t").schema(logsSchema)
      .csv("/user/pakhtyamov/data/user_logs/user_logs_M/logsLM.txt")

    val userSchema = Encoders.product[User].schema
    val usersDF = sqlContext.read.option("sep", "\t").schema(userSchema)
      .csv("/user/pakhtyamov/data/user_logs/user_data_M/userDataM")

    val ansDF = rowLogsDF.join(usersDF, "ip").select("sex", "age", "ip").groupBy("sex", "age")
      .agg(countDistinct("ip")).as("count")

    val fs = FileSystem.get(new Configuration())
    val outputWriter = new PrintWriter(fs.create(new Path("/user/esidorov/hw5/dataframes/task4")))

    outputWriter.println(ansDF.columns.mkString(","))
    outputWriter.println()
    ansDF.collect().foreach(outputWriter.println)
    outputWriter.flush()
    outputWriter.close()


  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

  case class User(ip: String, browser: String, sex: String, age: Long)

}
