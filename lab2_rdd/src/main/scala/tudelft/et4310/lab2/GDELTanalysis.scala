package tudelft.et4310.lab2

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.io.Source

object GDELTanalysis {

  def main(args: Array[String]) {

    // Get SparkSession and SparkContext
    val spark = SparkSession
      .builder
      .appName("GDELTanalysis")
      .getOrCreate()
    val sc = spark.sparkContext

    // Import data (filenames file will be placed in same folder as .jar on S3 bucket)
    // Take only 10 for now
    val gkgFiles = Source.fromFile("gdeltv2gkg.txt").getLines.take(10).mkString(",")
    val rdd = sc
      .textFile(gkgFiles) // each file is 1 partition
      .coalesce(32) // decrease partitions to number of cores and minimize shuffle read/write

    // flatMap rows to array of (publishDate, name) + filter (as early as possible = better)
    val data = rdd
      .filter(row => row.split("\t", -1).length == 27) // no need to fuse this! (=filter inside flatMap)
      .flatMap(row => {
      val columns = row.split("\t", -1)
      val publishDate = columns(1).substring(0, 8) // take only yyyymmdd
      columns(23)
        .split(";", -1)
        .map(names => {
          val name = names.split(",")(0) // take only name, not offset
          (publishDate, name)
        })
        .filter(x => x._2 != "" || x._2 != "Type ParentCategory") // filter for bad names
    })

    // Aggregate by key using hashmaps
    val zeroValue = new mutable.HashMap[String, Int]() {
      override def default(key: String) = 0 // overwrite default method for HashMap (to get 0 when key absent)
    }

    def seqOp(accumCount: mutable.HashMap[String, Int], key: String): mutable.HashMap[String, Int] = {
      accumCount += (key -> (accumCount(key) + 1)) // increment count if key already in there, else set count to 1
    }

    def combOp(accum1: mutable.HashMap[String, Int], accum2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
      accum2.foreach { case (k, v) => accum1 += (k -> (accum1(k) + v)) } // merge two hashmaps by adding counts
      accum1
    }

    val newestResult = data
      .aggregateByKey(zeroValue)(seqOp, combOp)
      .mapValues(value => value
        .toList // convert HashMap to List
        .sortBy(-_._2) // sort descending based on count
        .take(10)) // take 10 highest

    // Save
    newestResult.saveAsTextFile("../output_rdd")

    // Quit
    spark.stop
  }
}
