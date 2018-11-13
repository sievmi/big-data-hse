/**
  * Created by sievmi on 13.11.18  
  */

import org.apache.spark.{SparkContext, _}

object SparkWordCount {
  def main(args: Array[String]) {

    // val sc = new SparkContext( "local", "Word Count", "/usr/local/spark", Nil, Map(), Map())
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("local app")
    val sc: SparkContext = new SparkContext(conf)

    /* local = master URL; Word Count = application name; */
    /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */
    /* Map = variables to work nodes */
    /*creating an inputRDD to read text file (in.txt) through Spark context*/
    val input = sc.textFile("/home/esidorov/input.txt")
    /* Transform the inputRDD into countRDD */

    val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    /* saveAsTextFile method is an action that effects on the RDD */
    count.saveAsTextFile("outfile")
    System.out.println("OK")
  }
}
