package tudelft.et4310.lab1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object GDELTanalysis {

  def main(args: Array[String]) {

    // Prevent too much console output
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Get SparkSession and SparkContext
    val spark = SparkSession
      .builder
      .appName("GDELTanalysis")
      .master("local[8]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Import data (lab1_rdd is root folder)
    val rdd = sc.textFile("../data/segment/*.csv") // each file is 1 partition

    // Map
    //    val data = rdd.flatMap(row => {
    //      if (row.split("\t", -1).length != 27) None else {
    //        val columns = row.split("\t", -1)
    //        val publishDate = columns(1).substring(0, 8)
    //
    //        columns(23).split(";", -1).map(occurence => {
    //          val name = occurence.split(",")(0)
    //          (publishDate, name) // necessary? we could also increment based on name
    //        }).filterNot(x => x._2 == "" || x._2 == "Type ParentCategory") // filter as early as possible
    //      }
    //    })

    // Map + filter (as early as possible)
    // Approach 1
    //    val data = rdd
    //      .filter(row => row.split("\t", -1).length == 27) // filter for bad rows
    //      .flatMap(row => {
    //      val columns = row.split("\t", -1)
    //      val publishDate = columns(1).substring(0, 8)
    //
    //      columns(23).split(";", -1).map(names => {
    //        val name = names.split(",")(0)
    //        (publishDate, name)
    //      }).filter(x => x._2 != "" || x._2 != "Type ParentCategory") // filter bad names
    //    })

    // Approach 2
    val data = rdd
      .flatMap(row => {
        val columns = row.split("\t", -1)

        if (columns.length != 27) None else { // check for bad rows
          val publishDate = columns(1).substring(0, 8)

          columns(23).split(";", -1).map(names => {
            val name = names.split(",")(0)
            (publishDate, name)
          }).filter(kv => kv._2 != "" && kv._2 != "Type ParentCategory") // filter bad names
        }
      })

    // Reduce with groupByKey
    //    val result = data
    //      .reduceByKey(_ + _)
    //      .map(x => (x._1._1, (x._1._2, x._2)))
    //      .groupByKey
    //      .mapValues(_.toList.sortBy(-_._2).take(10)) // sort by -value to do descending!

    // Reduce without groupByKey:
    //    val dataReduced = data
    //      .reduceByKey(_ + _)
    //      .map(x => (x._1._1, (x._1._2, x._2)))

    // Newest
    val zeroValue = new mutable.HashMap[String, Int]() {
      override def default(key: String) = 0
    }

    def seqOp(accumCount: mutable.HashMap[String, Int], value: String): mutable.HashMap[String, Int] = {
      accumCount += (value -> (accumCount(value) + 1))
    }

    def combOp(accum1: mutable.HashMap[String, Int], accum2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
      accum2.foreach { case (k, v) => accum1 += (k -> (accum1(k) + v)) }
      accum1
    }

    def combOpV2(accum1: mutable.HashMap[String, Int], accum2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
      accum1 ++= accum2.map { case (k, v) => k -> (accum1(k) + v) }
      accum1
    }

    //    val newestResult = data.aggregateByKey(firstLetM)(fSeqM, fCombM).mapValues(_.toList.sortBy(-_._2).take(10))
    val newestResult = data.aggregateByKey(zeroValue)(seqOp, combOpV2).mapValues(_.toList.sortBy(-_._2).take(10))


    // With HashMap
    //    val initialMap = mutable.HashMap.empty[String, Int] // create template
    //    val addToMap = (map: mutable.HashMap[String, Int], value: (String, Int)) => map += value
    //    val mergeMaps = (part1: mutable.HashMap[String, Int],
    //                     part2: mutable.HashMap[String, Int]) => part1 ++= part2
    //    val betterResultMap = data.aggregateByKey(initialMap)(addToMap, mergeMaps)
    //      .mapValues(_.toList.sortBy(-_._2).take(10))
    //
    //    // With HashSet
    //    val initialSet = mutable.HashSet.empty[(String, Int)]
    //    val addToSet = (set: mutable.HashSet[(String, Int)], value: (String, Int)) => set += value
    //    val mergeSets = (part1: mutable.HashSet[(String, Int)],
    //                     part2: mutable.HashSet[(String, Int)]) => part1 ++= part2
    //    val betterResultSet = data.aggregateByKey(initialSet)(addToSet, mergeSets)
    //      .mapValues(_.toList.sortBy(-_._2).take(10))

    // Other possibility: dont reduce first, but aggregatebykey on date: hashmap, +1 in case of key already in there, new if key not in there yet

    // Collect results (action!)
    //    result.collect().foreach(println)
    //    betterResultMap.collect().foreach(println)
    //    betterResultSet.collect().foreach(println)
    //    newestResult.collect().foreach(println)

    // Show and save
    //    newestResult.collect().foreach(println)
    val writeResult = newestResult.repartition(1)
    writeResult.saveAsTextFile("../output")
    //    newestResult2.map()

    // Quit
    spark.stop
  }
}
