package dataframe.task5

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
  * Created by sievmi on 16.11.18  
  */
object IpUdf {
  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf().setAppName("IpUdf").setMaster("yarn")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val splitIp = (ip: String) => ip.split('.').map(_.toInt).mkString(" ")
    val splitUdf = udf(splitIp)

    val userSchema = Encoders.product[User].schema
    val usersDF = sqlContext.read.option("sep", "\t").schema(userSchema)
      .csv("/user/pakhtyamov/data/user_logs/user_data_M/userDataM")


    val usersRDD = usersDF.withColumn("splitted", splitUdf(col("ip"))).rdd
    val newSchema = StructType(usersDF.schema.fields.init ++
      Array(StructField("ip_1", IntegerType), StructField("ip_2", IntegerType),
        StructField("ip_3", IntegerType), StructField("ip_4", IntegerType)))
    val newRDD = usersRDD.map(r => Row.fromSeq(
      r.toSeq.init ++ r.getAs[String]("splitted").split(' ')))
    sqlContext.createDataFrame(newRDD, newSchema)

    newRDD.saveAsTextFile("/user/esidorov/hw5/dataframes/task5")
  }

  case class UserLog(ip: String, c1: String, c2: String, c3: String, url: String,
                     c5: String, status: String, browser: String)

  case class User(ip: String, browser: String, sex: String, age: Long)

}
