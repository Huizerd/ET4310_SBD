package tudelft.et4310.lab1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object GDeltAnalysis {

  def main(args: Array[String]) {

    // Prevent too much console output
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Get SparkSession and SparkContext
    val spark = SparkSession
      .builder
      .appName("GDeltAnalysis")
      .master("local[8]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Import data (lab1_rdd is root folder)
    val rdd = sc.textFile("../data/segment/*.csv") // each file is 1 partition

    // Map
    val data = rdd.flatMap(row => {
      val columns = row.split("\t", -1)
      val publishDate = columns(1).substring(0, 8)

      columns(23).split(";", -1).map(occurence => {
        val name = occurence.split(",")(0)
        ((publishDate, name), 1)
      })
    })

    // Reduce with groupByKey
    val result = data
      .reduceByKey(_ + _)
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey
      .mapValues(_.toList.filterNot(x => x._1 == "" || x._1 == "Type ParentCategory").sortBy(-_._2).take(10)) // sort by -value to do descending!

    // Reduce without groupByKey
    val dataReduced = data
      .reduceByKey(_ + _)
      .partitionBy(new HashPartitioner(8)).persist() // 10 -> 8 partitions (saves 1s) --> https://github.com/rohgar/scala-spark-4/wiki/Optimizing-with-Partitioners
      .map(x => (x._1._1, (x._1._2, x._2)))

    // With HashMap
    val initialMap = mutable.HashMap.empty[String, Int] // create template
    val addToMap = (map: mutable.HashMap[String, Int], value: (String, Int)) => map += value
    val mergeMaps = (part1: mutable.HashMap[String, Int],
                     part2: mutable.HashMap[String, Int]) => part1 ++= part2
    val betterResultMap = dataReduced.aggregateByKey(initialMap)(addToMap, mergeMaps)
      .mapValues(_.toList.filterNot(x => x._1 == "" || x._1 == "Type ParentCategory").sortBy(-_._2).take(10))

    // With HashSet
    val initialSet = mutable.HashSet.empty[(String, Int)]
    val addToSet = (set: mutable.HashSet[(String, Int)], value: (String, Int)) => set += value
    val mergeSets = (part1: mutable.HashSet[(String, Int)],
                     part2: mutable.HashSet[(String, Int)]) => part1 ++= part2
    val betterResultSet = dataReduced.aggregateByKey(initialSet)(addToSet, mergeSets)
      .mapValues(_.toList.filterNot(x => x._1 == "" || x._1 == "Type ParentCategory").sortBy(-_._2).take(10))

    // Collect results (action!)
    result.collect().foreach(println)
    betterResultMap.collect().foreach(println)
    betterResultSet.collect().foreach(println)

    spark.stop
  }
}
