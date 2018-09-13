package example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._


object ExampleSpark {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("GDELThist")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    // Import data
    val rdd = sc.textFile("/home/huis/Projects/ET4310_SBD/data/segment/*.csv")

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
    //    val data_red = data.reduceByKey()
    val result = data.reduceByKey(_ + _)

    result.collect().foreach(println)
    spark.stop
  }
}
