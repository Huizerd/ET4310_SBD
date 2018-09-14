package tudelft.et4310.lab1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GDeltAnalysis {

  def main(args: Array[String]) {

    // Prevent too much console output
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Get SparkSession and SparkContext
    val spark = SparkSession
      .builder
      .appName("GDeltAnalysis")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    // Import data (lab1_rdd is root folder)
    val rdd = sc.textFile("../data/segment/*.csv")

    // Map
    //    val data = rdd.map(row => (row.split("\t")(1), row.split("\t")(23)))
    //      .flatMapValues(names => names.split(";"))  //.split(";").map(name => (name(0), 1))))
    //      .mapValues(name => (name.split(",")(0), 1))
    val data = rdd.flatMap(row => {
      val columns = row.split("\t", -1)
      val publishDate = columns(1).substring(0, 8)

      columns(23).split(";", -1).map(occurence => {
        val name = occurence.split(",")(0)
        ((publishDate, name), 1)
      })
    })

    // Reduce
    val result = data.reduceByKey(_ + _).map(x => (x._1._1, (x._1._2, x._2))).groupByKey.mapValues(_.toList)

    // Collect results (action!)
    result.collect()

    spark.stop
  }
}
