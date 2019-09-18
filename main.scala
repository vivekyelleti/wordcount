/**
 * Illustrates flatMap + countByValue for wordcount.
 */

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}

object WordCount {
    def main(args: Array[String]) {
      //val inputFile = args(0)
      val conf = new SparkConf().setMaster("spark://localhost:7070").setAppName("wordcount")
	conf.set("spark.executor.memory", "4g")
    	val sc = new SparkContext(conf)
    	val rootLogger = Logger.getRootLogger()
    	rootLogger.setLevel(Level.ERROR)
      

      // Load our input data.
      val input =  sc.textFile("/home/vivek/Project-Docs/vivek/Spark/file.txt")
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("/home/vivek/Project-Docs/vivek/Spark/output")
    }
}
