import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sievmi on 12.11.18  
  */
class Main {

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("local app")
  val sc: SparkContext = new SparkContext(conf)

  val wikiRdd: RDD[String] = sc.textFile(new File("").getPath, 1)

}
