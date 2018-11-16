package dataframe.task5

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions.{udf, col}

/**
  * Created by sievmi on 16.11.18  
  */
object IpUdf {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("IpUdf").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val splitIp = (ip: String) => ip.split(".").map(_.toInt)
    val splitUdf = udf(splitIp)

    val userSchema = Encoders.product[User].schema
    val usersDF = sqlContext.read.option("sep", "\t").schema(userSchema)
      .csv("/user/pakhtyamov/data/user_logs/user_data_M/userDataM")
      .withColumn("splitted", splitUdf(col("ip")))

    usersDF.rdd.saveAsTextFile("/user/esidorov/hw5/dataframes/task5")
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

  case class User(ip: String, browser: String, sex: String, age: Long)

}
